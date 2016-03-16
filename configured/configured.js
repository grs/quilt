/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

var amqp = require('rhea');
var kube = require('./kube_utils.js');

var routers = {};
var brokers = {};

function router_service_url() {
    return process.env.ROUTER_SERVICE_HOST + ':' + process.env.ROUTER_SERVICE_PORT;
}

function check_full_mesh() {
    //ensure that each router in the list is connected to all the others
    //if not, add connectors as needed
    for (var r in routers) {
        for (var other in routers) {
            if (r !== other) {
                routers[r].ensure_connected(routers[other].listener);
            }
        }
    }
}

var Router = function (container_id, connection) {
    this.listener = undefined;
    this.container_id = container_id;
    this.connected = {};//map of router address to a 'direction'
    this.connection = connection;
    this.sender = connection.open_sender('$management');
    this.counter = 0;
    this.requests = {};
    connection.open_receiver({source:{dynamic:true}});
    connection.on('receiver_open', this.init.bind(this));
    connection.on('message', this.incoming.bind(this));
};

Router.prototype.disconnected = function () {
    this.connection = undefined;
    this.sender = undefined;
};

Router.prototype._update_connected = function (key, direction) {
    if (this.connected[key]) {
        if (this.connected[key] !== direction) {
            console.log('INFO: ' + this.listener + ' and ' + key + ' connected in both directions');
        }
    } else {
        this.connected[key] = direction;
    }
};

Router.prototype.connected_to = function (key) {
    this._update_connected(key, 'out');
};

Router.prototype.connected_from = function (key) {
    this._update_connected(key, 'in');
};

Router.prototype.ensure_connected = function (key) {
    if (!this.connected[key]) {
        var parts = key.split(':');
        this.create_entity('connector', key, {role:'inter-router', addr:parts[0], port:parts[1], idleTimeoutSeconds:0});
    }
};

Router.prototype.init = function (context) {
    this.address = context.receiver.remote.attach.source.address;
    this.query('listener', {attributeNames:['identity', 'name', 'addr', 'port', 'role']});
};

Router.prototype.request = function (operation, properties, body, callback) {
    if (this.sender) {
	this.counter++;
	var id = this.counter.toString(); 
	var req = {properties:{reply_to:this.address, correlation_id:id}};
        req.application_properties = properties || {};
        req.application_properties.operation = operation;
        req.body = body;
	this.requests[id] = callback || function (response) { console.log('response: ' + JSON.stringify(req) + ' => ' + JSON.stringify(response)); };
        this.sender.send(req);
    }
};

Router.prototype._get_callback = function (operation, type) {
    var method = this['on_' + operation + '_' + type + '_response'];
    if (method) return method.bind(this);
    else return undefined;
}

Router.prototype.query = function (type, options, callback) {
    this.request('QUERY', {entityType:type}, options || {attributeNames:[]}, callback || this._get_callback('query', type));
};

Router.prototype.create_entity = function (type, name, attributes, callback) {
    this.request('CREATE', {'type':type, 'name':name}, attributes || {}, callback || this._get_callback('create', type));
};

// e.g. this.delete_entity({type:'connector', name:'router-b'});
Router.prototype.delete_entity = function (identifier, callback) {
    this.request('DELETE-' + identifier.type, 'DELETE', identifier, {}, callback || this._get_callback('delete', type));
};

function foreach_result(body, f) {
    var results = body.results;
    var names = body.attributeNames;
    for (var i = 0; i < results.length; i++) {
        var record = {};
        for (var j = 0; j < names.length; j++) {
            record[names[j]] = results[i][j];
        }
        f(record);
    }
};

Router.prototype._process_connector = function (record) {
    if (record.role === 'inter-router') {
        var key = record.addr + ':' + record.port;
        this.connected_to(key);
        if (routers[key]) {
            routers[key].connected_from(this.listener);
        } else {
            console.log('WARNING: ' + this.listener + ' has inter-router connector to ' + key + ', for which no matching router has connected');
        }
    }
};

Router.prototype.on_query_connector_response = function (message) {
    foreach_result(message.body, this._process_connector.bind(this));
    console.log('Updated connectors for ' + this.listener + ': ' + JSON.stringify(this.connected));
    check_full_mesh();
};

Router.prototype.on_query_listener_response = function (message) {
    var listeners = [];
    foreach_result(message.body, function (record) { if (record.role === 'inter-router') listeners.push(record.addr + ':' + record.port)});
    if (listeners.length > 0) {
        for (var i = 0; i < listeners.length; i++) {
            if (i === 0) {
                this.listener = listeners[i];
            }
            routers[listeners[i]] = this;
            console.log('Added ' + this.listener + ' to known routers');
        }
    } else {
        console.log('WARNING: router connected with no preconfigured listener');
    }
    this.query('connector', {attributeNames:['identity', 'name', 'addr', 'port', 'role']});
};

Router.prototype.on_create_connector_response = function (message) {
    if (message.application_properties.statusDescription === 'Created') {
        console.log('Connector creation succeeded, requerying...');
        //requery
        this.query('connector', {attributeNames:['identity', 'name', 'addr', 'port', 'role']});
    } else {
        console.log('ERROR: ' + JSON.stringify(message));
    }
};

Router.prototype.on_delete_connector_response = function (message) {
    console.log('connector deletion response: ' + context.message.application_properties.statusDescription);
};

Router.prototype.incoming = function (context) {
    //console.log('Got message: ' + JSON.stringify(context.message));
    var message = context.message;
    var handler = this.requests[message.properties.correlation_id];
    if (handler) {
	delete this.requests[message.properties.correlation_id];
	handler(message);
    } else {
	console.log('WARNING: unexpected response: ' + message.properties.correlation_id + ' [' + JSON.stringify(message) + ']');
    }
};

Router.prototype._define_fixed_address = function (address, phase, callback) {
    this.create_entity('fixedAddress', address.name+'_'+phase, {'prefix':address.name, 'phase':phase, 'fanout':'single'}, callback);
};

Router.prototype._define_ondemand_connector = function (name, broker, callback) {
    this.create_entity('connector', name, {'role':'on-demand', 'addr':broker.host, 'port':broker.port}, callback);
};

Router.prototype._define_waypoint = function (address, connector, callback) {
    this.create_entity('waypoint', address.name, {'address':address.name, 'inPhase':0, 'outPhase':1, 'connector':connector}, callback);
};

Router.prototype.define_address_sync = function (address) {
    if (address.type === 'queue') {
	var router = this;
	var waypoint = address.waypoints[this.listener];
	router._define_fixed_address(address, 0, function() {
	    console.log('phase 0 address for ' + address.name + ' created on ' + router.container_id);
	    router._define_fixed_address(address, 1, function() {		    
		console.log('phase 1 address for ' + address.name + ' created on ' + router.container_id);
		if (waypoint) {
		    console.log('defining waypoint for ' + address.name + ' on ' + router.container_id);
		    var broker = waypoint.brokers[0];//can only handle one broker per waypoint at present
		    var connector_id = address.name + '_' + broker.container_id;
		    router._define_ondemand_connector(connector_id, broker, function() {
			console.log('created connector ' + connector_id + ' on ' + router.container_id);
			router._define_waypoint(address, connector_id, function() {
			    console.log('created waypoint for ' + address.name + ' on ' + router.container_id);
			});
		    });
		}
	    }); 
	});
    }
    //TODO other address types
};

Router.prototype.define_address_async = function (address) {
    if (address.type === 'queue') {
	this.create_entity('fixedAddress', address.name+'_0', {prefix:address.name, phase:0, fanout:'single'});
	this.create_entity('fixedAddress', address.name+'_1', {prefix:address.name, phase:1, fanout:'single'});
	if (address.waypoints[this.listener]) {		    
	    //define waypoint on one router for each broker shard:
	    var broker = address.waypoints[this.listener].brokers[0];//can only handle one broker per waypoint at present
	    var id = address.name + '_' + broker.container_id;
	    this.create_entity('connector', id, {role:'on-demand', addr:broker.host, port:broker.port});
	    this.create_entity('waypoint', address.name, {address:address.name, inPhase:0, outPhase:1, connector:id});		    
	}
    }
    //TODO other address types
};

var Queue = function (name, attributes) {
    this.name = name;
    this.attributes = attributes || {durable:true};
    this.state = 'CREATING';
    this.linked_in = false;
    this.linked_out = false;
}

Queue.prototype.created = function () {
    this.state = 'CREATED';
};

Queue.prototype.remove = function () {
    this.state = 'DELETING';
};

var Qpidd = function (connection) {
    this.container_id = connection.remote.open.container_id;
    this.type = 'qpidd';
    this.queues = {};
    this.connected(connection);
    this.requests = {};
    this.counter = 0;
};

Qpidd.prototype.get_load = function () {
    return Object.keys(this.queues).length;
};

Qpidd.prototype.connected = function (connection) {
    this.connection = connection;
    this.host = connection.options.host || 'localhost';
    this.port = connection.options.port || 5672;
    this.sender = connection.open_sender('qmf.default.direct');
    connection.open_receiver({source:{dynamic:true}});
    connection.on('receiver_open', this.init.bind(this));
    connection.on('message', this.incoming.bind(this));
};

Qpidd.prototype._request = function (callback, opcode) {
    this.counter++;
    var id = this.counter.toString(); 
    this.requests[id] = callback;
    var request = {properties:{correlation_id:id,subject:'broker',reply_to:this.address}};
    request.application_properties = {'x-amqp-0-10.app-id':'qmf2', 'qmf.opcode':opcode};
    return request;
}

Qpidd.prototype._query_request = function (type, callback) {
    var request = this._request(callback, '_query_request');
    request.body = {'_what':'OBJECT','_schema_id':{'_class_name':type}};
    return request;
};

Qpidd.prototype._method_request = function (callback, method) {
    var request = this._request(callback, '_method_request');
    request.body = {'_object_id':{'_object_name':'org.apache.qpid.broker:broker:amqp-broker'},'_method_name':method};
    return request;
};

Qpidd.prototype._create_request = function (type, name, attributes, callback) {
    var request = this._method_request(callback, 'create');
    request.body._arguments = {'type':type, 'name':name, properties: attributes};
    return request;
};

Qpidd.prototype._delete_request = function (type, name, callback) {
    var request = _method_request(callback, 'delete');
    request.body._arguments = {'type':type, name:'name'};
    return request;
};

Qpidd.prototype.init = function (context) {
    this.address = context.receiver.remote.attach.source.address;
    //create domain for router service
    var request = this._create_request('domain', 'qdrouterd', {url:router_service_url(), sasl_mechanisms:'ANONYMOUS'}, this.on_domain_create_response.bind(this, 'qdrouterd'));
    console.log('Sending ' + JSON.stringify(request));
    this.sender.send(request);
    //this.sender.send(this._create_request('domain', 'qdrouterd', {url:router_service_url(), sasl_mechanisms:'ANONYMOUS'}), this.on_domain_create_response.bind(this, 'qdrouterd'));
    //query queues
    request = this._query_request('queue', this.on_queue_query_response.bind(this))
    console.log('Sending ' + JSON.stringify(request));
    this.sender.send(request);
    //this.sender.send(this._query_request('queue', this.on_queue_query_response.bind(this)));
};

function inphase(name) {
    return 'M0' + name;
}

function outphase(name) {
    return 'M1' + name;
}

Qpidd.prototype.incoming = function (context) {
    var message = context.message;
    var handler = this.requests[message.properties.correlation_id];
    if (handler) {
	delete this.requests[message.properties.correlation_id];
	handler(message);
    } else {
	console.log('WARNING: unexpected response: ' + message.properties.correlation_id + ' [' + JSON.stringify(message) + ']');
    }
};

Qpidd.prototype.on_domain_create_response = function (name, message) {
    if (message.application_properties['qmf_opcode'] === '_exception') {
	console.log('ERROR: failed to create domain: ' + message.body);
    } else {
	console.log('Created domain \'' + name + '\' on ' + this.container_id);
    }
}

Qpidd.prototype.on_incoming_create_response = function (q, message) {
    if (message.application_properties['qmf_opcode'] === '_exception') {
	console.log('ERROR: failed to create incoming link: ' + message.body);
    } else {
	console.log('Created incoming link for \'' + q.name + '\' on ' + this.container_id);
    }
}

Qpidd.prototype.on_outgoing_create_response = function (q, message) {
    if (message.application_properties['qmf_opcode'] === '_exception') {
	console.log('ERROR: failed to create outgoing link: ' + message.body);
    } else {
	console.log('Created outgoing link for \'' + q.name + '\' on ' + this.container_id);
    }
}

Qpidd.prototype.on_queue_create_response = function (q, message) {
    if (message.application_properties['qmf_opcode'] === '_exception') {
	console.log('ERROR: failed to create queue: ' + message.body);
    } else {
	console.log('Created queue \'' + q.name + '\' on ' + this.container_id);
	q.created();
	//this.link_queue(q);
    }
};

Qpidd.prototype.on_queue_delete_response = function (q, message) {
    if (message.application_properties['qmf_opcode'] === '_exception') {
	console.log('ERROR: failed to delete queue: ' + message.body);
    } else {
	delete this.queues[q.name];
    }
}

Qpidd.prototype.add_queue = function (q) {
    this.queues[q.name] = q;
    this.sender.send(this._create_request('queue', q.name, q.attributes, this.on_queue_create_response.bind(this, q)));
};

Qpidd.prototype.link_queue = function (q) {
    //create incoming link, i.e. for messages travelling from router to broker:
    this.sender.send(this._create_request('incoming', q.name + '_in', {domain:'qdrouterd', source:inphase(q.name), target:q.name}, this.on_incoming_create_response.bind(this, q)));
    //create outgoing link, i.e. for messages travelling from broker to router:
    this.sender.send(this._create_request('outgoing', q.name + '_out', {domain:'qdrouterd', source:q.name, target:outphase(q.name)}, this.on_outgoing_create_response.bind(this, q)));
}

Qpidd.prototype.on_queue_query_response = function (message) {
    var exists = {};
    for (var i = 0; i < message.body.length; i++) {
	var q = message.body[i]['_values'];
	exists[q.name] = q;
    }
    var pending = [];
    for (var name in this.queues) {
	var q = this.queues[name];
	if (exists[name]) {
	    switch (q.state) {
	    case 'CREATED':
		//nothing to do
		break;
	    case 'CREATING':
		q.created();
		break;
	    case 'DELETING':
		pending.push(this._delete_request('queue', q.name, this.on_queue_delete_response.bind(this, q)));
		break;
	    }
	} else {
	    switch (q.state) {
	    case 'CREATED':
		console.log('WARNING: queue ' + q.name + ' not found on ' + this.container_id + '; recreating');
		//drop through...
	    case 'CREATING':
		pending.push(this._create_request('queue', q.name, q.attributes, this.on_queue_create_response.bind(this, q)));
		break;
	    case 'DELETING':
		delete this.queues[name];
		break;
	    }
	}
    }
    for (var i = 0; i < pending.length; i++) {
	this.sender.send(pending[i]);
    }
};

Qpidd.prototype.disconnected = function (context) {
    console.log(this.container_id + ' disconnected');
};

/**
 * Returns an array of 'n' of the least 'loaded' brokers.
 */
function allocate_brokers(n) {
    var result = [];
    for (var id in brokers) {
	var b = brokers[id];
	if (result.length < n || result[result.length-1].get_load() > b.queues.get_load()) {
	    result.pop();
	    result.push(b);
	    result.sort(function (a, b) { return a.get_load() - b.get_load(); });
	}
    }
    return result;
};

/**
 * Returns map with up to n random key-value pairs from the original map.
 */
function pick_random(map, n) {
    var keys = Object.keys(map);
    var result = {};
    if (keys.length <= n) {
	result = map;
    } else if (keys.length > 0) {
	for (var i = 0; i < n; i++) {
	    var chosen;
	    do {
		chosen = keys[Math.round(Math.random() * (keys.length - 1))];
	    } while (result[chosen]);
	    result[chosen] = map[chosen];
	}
    }
    return result;
}

function allocate_waypoints(shards) {
    var chosen_routers = pick_random(routers, shards.length);
    var router_keys = Object.keys(chosen_routers);
    var results = {};
    for (var i = 0; i < shards.length; i++) {
	var r = router_keys[i % router_keys.length];
	if (results[r]) {
	    results[r].brokers.push(shards[i]);
	} else {
	    results[r] = {router:chosen_routers[r], brokers:[shards[i]]}
	}
    }
    return results;
}

function configuration_request(context) {
    console.log('Received configuration request: ' + JSON.stringify(context.message));
    if (context.message.properties.subject === 'define') {
	var address = context.message.body;
	if (address.type === 'queue') {
	    address.brokers = allocate_brokers(address.shards || 1);
	    address.waypoints = allocate_waypoints(address.brokers);
	    console.log('defining queue ' + address.name + ' on broker(s):');
	    for (var b in address.brokers) {
		console.log('        ' + address.brokers[b].container_id);
		address.brokers[b].add_queue(new Queue(address.name));
	    }
	    console.log('    and accompanying waypoints on routers:');
	    for (var r in routers) {
		routers[r].define_address_sync(address);
		console.log('        ' + routers[r].container_id);
	    }
	} else {
	    console.log('unhandled address type ' + JSON.stringify(address));
	}
    } else if (context.message.properties.subject === 'connect') {
	amqp.connect(context.message.body);
	console.log('connecting... ' + JSON.stringify(context.message.body) );
    }
}

function get_product(connection) {
    if (connection.remote.open.properties) {
	return connection.remote.open.properties.product;
    } else {
	return undefined;
    }
}

amqp.on('connection_open', function(context) {
    var product = get_product(context.connection);
    if (product === 'qpid-dispatch-router') {
        var r = new Router(context.connection.remote.open.container_id, context.connection);
        console.log('Router connected from ' + context.connection.remote.open.container_id);
        context.connection.on('connection_close', function(context) {
	    //TODO: delete after some time to allow for reconnection
	    //routers[id].disconnected();
        });
        context.connection.on('disconnect', function(context) {
	    //TODO: delete after some time to allow for reconnection
	    //routers[id].disconnected();
        });
    } else if (product === 'qpid-cpp') {
	var id = context.connection.remote.open.container_id;
	if (brokers[id] === undefined) {
	    brokers[id] = new Qpidd(context.connection);
	    console.log('Connection established from qpidd: ' + id + ' [' + context.connection.id + ']');
	} else {
	    brokers[id].connected(context.connection);
	    console.log('Connection re-established from qpidd: ' + id + ' [' + context.connection.id + ']');
	}
    } else {
	context.connection.on('message', configuration_request);
    }
});


amqp.sasl_server_mechanisms.enable_anonymous();
amqp.listen({port:55672});
var watcher = kube.watch_service('brokers');
watcher.on('added', function (procs) {
    for (var name in procs) {
	var proc = procs[name];
	proc.id = name;
	amqp.connect(proc);
	console.log('connecting to new broker on ' + JSON.stringify(proc));
    }

});
watcher.on('removed', function (procs) {
    console.log('brokers removed from service: ' + JSON.stringify(procs));
});
