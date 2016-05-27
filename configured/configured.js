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
var http = require('http');
var url = require('url');
var kube = require('./kube_utils.js');
var qpidd = require('./qpidd_utils.js');
var artemis = require('./artemis_utils.js');

var routers = {};
var brokers = {};

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
    this.brokers = {};
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

function fully_qualified_type(type) {
    return "org.apache.qpid.dispatch.router.config." + type;
}

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


Router.prototype.define_address = function (address) {
    var dist = address.spec.multicast ? "multicast" : "balanced";
    console.log('requesting address: ' + address.name);
    this.create_entity('router.config.address', address.name, {prefix:address.name, distribution:dist, waypoint:address.spec.store_and_forward});
    if (address.spec.store_and_forward) {
	for (var b in address.brokers) {
	    var broker = address.brokers[b].container_id;
	    console.log('requesting auto links with ' + broker);
	    this.create_entity('router.config.autoLink', address.name + '-to-' + broker, {addr:address.name, dir:"out", connection:broker});
	    this.create_entity('router.config.autoLink', address.name + '-from-' + broker, {addr:address.name, dir:"in", connection:broker});
	}
    }
};

Router.prototype.connect_to_broker = function (broker) {
    this.brokers[broker.container_id] = broker;
    this.create_entity('connector', broker.container_id, {addr:broker.host, port:broker.port, role:'route-container'});
}

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

function get_router_for_broker() {
    var result = undefined;
    for (var id in routers) {
	var r = routers[id];
	if (result === undefined || r.brokers.length < result.brokers.length) {
	    result = r;
	}
    }
    return result;
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

function allocate_router_for_shards(shards) {
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

function define_address(name, details) {
    var address = {};
    address.name = name;
    address.spec = details;    
    if (details.store_and_forward) {
	address.brokers = allocate_brokers(details.shards || 1);
	if (details.multicast) {
	    console.log('brokered topic not yet supported');
	} else {
	    console.log('defining queue ' + address.name + ' on broker(s):');
	    for (var b in address.brokers) {
		console.log('        ' + address.brokers[b].container_id);
		address.brokers[b].add_queue(new Queue(address.name));
	    }
	}
    }
    console.log('defining address ' + address.name + ' on router(s):');
    for (var r in routers) {
	routers[r].define_address(address);
	console.log('xxx');
	console.log('        ' + routers[r].container_id + ' (' + r +')');
    }
}

function delete_address(name, details) {
    //TODO
    console.log('FIXME: deletion not yet implemented');
}

function configuration_request(context) {
    console.log('Received configuration request: ' + JSON.stringify(context.message));
    if (context.message.properties.subject === 'define') {
	var address = context.message.body;
	if (address.type === 'queue') {
	    define_queue(address.name, address);
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

function add_broker(broker) {
    var id = broker.container_id;
    brokers[id] = broker;
    console.log('Connection established from ' + broker.type + ': ' + id + ' [' + broker.connection.options.id + ']');
    var router = get_router_for_broker();
    if (router) {
	router.connect_to_broker(broker);
    } else {
	console.log('ERROR: no routers available to connect to broker');
    }
}

function handle_broker_connection(connection, Broker) {
    var id = context.connection.remote.open.container_id;
    if (id === undefined || brokers[id] === undefined) {
	add_broker(new Broker(context.connection));
    } else {
	brokers[id].connected(context.connection);
	console.log('Connection re-established from ' + brokers[i].type + ': ' + id + ' [' + context.connection.options.id + ']');
	//TODO: alter connector for new ip of broker
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
	    add_broker(new qpidd.Qpidd(context.connection));
	} else {
	    brokers[id].connected(context.connection);
	    console.log('Connection re-established from qpidd: ' + id + ' [' + context.connection.options.id + ']');
	}
    } else if (product === undefined && context.connection.remote.open.container_id === '') {
	//temporary hack: identify artemis by lack of container id and
	//product information and use the connection identifier as the
	//broker id for now
	add_broker(new artemis.Artemis(context.connection));
    } else {
	context.connection.on('message', configuration_request);
    }
});


amqp.sasl_server_mechanisms.enable_anonymous();
amqp.listen({port:55672});

if (process.env.KUBERNETES_SERVICE_HOST) {
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
} else {
    console.log('Kubernetes service watcher was not activated.');
}

var addresses = {};

function address_updated(name, original, modified) {
    if (original === undefined) {
	console.log('address ' + name + ' set to ' + JSON.stringify(modified));
	define_address(name, modified);
    } else if (modified === undefined) {
	console.log('address ' + name + ' deleted (was ' + JSON.stringify(original) + ')');
	delete_address(name, original);
    } else {
	console.log('address ' + name + ' changed from ' + JSON.stringify(original) + ' to ' + JSON.stringify(modified));
	delete_address(name, original);
	define_address(name, modified);
    }
}

function write_response(response, content, code, content_type) {
    response.writeHead(code || 200, {'Content-Type': content_type || 'text/plain'});
    response.write(content);
    response.end();
}

function write_json_response(response, object, code, content_type) {
    write_response(response, JSON.stringify(object), code, 'application/json')
}

function http_error(code, text) {
    return { 'code': code, 'text': text };
}
function not_found(request) {
    return http_error(404, 'No such resource: ' + url.parse(request.url).pathname);
}
function bad_method(request) {
    return http_error(405, 'Resource ' + url.parse(request.url).pathname + ' does not support ' + request.method);
}

var handlers = {
    'address' : {
	do_get : function (request, response, resource) {
	    var a = addresses[resource.name];
	    if (a) {
		write_json_response(response, a);
	    } else {
		throw not_found(request);
	    }
	},
	do_put : function (request, response, resource) {
	    var buffer = '';
	    request.on('data', function (chunk) {
		buffer += chunk;
	    });
	    request.on('end', function () {
		try {
		    var a = JSON.parse(buffer);
		    var old = addresses[resource.name];
		    addresses[resource.name] = a;
		    process.nextTick(function () { address_updated(resource.name, old, a) });
		    write_json_response(response, a);
		} catch (error) {
		    write_response(response, error, 400);
		}
	    });
	},
	do_delete : function (request, response, resource) {
	    var old = addresses[resource.name];
	    delete addresses[resource.name];
	    process.nextTick(function () { address_updated(resource.name, old, undefined) });
	    response.writeHead(204);
	    response.end();
	}
    },
    'address_list' : {
	do_get : function (request, response) {
	    write_json_response(response, addresses);
	},
    },
    'connection_list' : {
	do_post : function (request, response) {
	    var buffer = '';
	    request.on('data', function (chunk) {
		buffer += chunk;
	    });
	    request.on('end', function () {
		try {
		    var details = JSON.parse(buffer);
		    amqp.connect(details);
		    console.log('connecting... ' + JSON.stringify(details));
		    write_response(response, 'OK');
		} catch (error) {
		    write_response(response, ''+error, 400);
		}
	    });

	},
    }
};

function resolve(resource) {
    if (resource.category === undefined) {
	return handlers[''];
    } if (resource.name === undefined) {
	return handlers[resource.category + '_list'] || handlers[resource.category];
    } else {
	return handlers[resource.category];
    }
}

function parse(request_url) {
    var parts = url.parse(request_url).pathname.match(/\/([^\/]+)(?:\/(.+))*/) || [];
    return { category: parts[1], name: parts[2] };
}

var server = http.createServer();
server.on('request', function (request, response) {
    try {
	var resource = parse(request.url);
	var handler = resolve(resource);
	if (handler) {
	    var method = handler['do_' + request.method.toLowerCase()];
	    if (method) {
		method.call(handler, request, response, resource);
	    } else {
		throw bad_method(request);
	    }
	} else {
	    throw not_found(request);
	}
    } catch (error) {
	write_response(response, error.text || ''+error, error.code || 500);
    }
});
server.listen(8080);
