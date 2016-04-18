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

module.exports.Qpidd = Qpidd;
