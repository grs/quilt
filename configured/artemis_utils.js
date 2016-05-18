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

var counter = 1;

var Artemis = function (connection) {
    // artemis doesn't yet pass a container id, so fake one for now
    //this.container_id = connection.remote.open.container_id;
    this.container_id = 'artemis' + counter++;
    this.type = 'artemis';
    this.queues = {};
    this.connected(connection);
};

Artemis.prototype.get_load = function () {
    return Object.keys(this.queues).length;
};

Artemis.prototype.connected = function (connection) {
    this.connection = connection;
    this.host = connection.options.host || 'localhost';
    this.port = connection.options.port || 5672;
    this.sender = connection.open_sender('jms.queue.activemq.management');
    connection.open_receiver({source:{dynamic:true}});
    connection.on('receiver_open', this.init.bind(this));
    connection.on('message', this.incoming.bind(this));
};

Artemis.prototype._request = function (resource, operation, parameters) {
    var request = {application_properties:{'JMSReplyTo': this.address, '_AMQ_ResourceName':resource, '_AMQ_OperationName':operation}};
    request.body = JSON.stringify(parameters);
    this.sender.send(request);
}

Artemis.prototype.deployQueue = function (name) {
    this._request('core.server', 'deployQueue', [name, name, null, true]);
}

Artemis.prototype.destroyQueue = function (name) {
    this._request('core.server', 'destroyQueue', [name]);
}

Artemis.prototype.init = function (context) {
    this.address = context.receiver.remote.attach.source.address;
};

Artemis.prototype.incoming = function (context) {
    var message = context.message;
};

Artemis.prototype.add_queue = function (q) {
    this.queues[q.name] = q;
    this.deployQueue(q.name);
};

Artemis.prototype.on_queue_query_response = function (message) {
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

Artemis.prototype.disconnected = function (context) {
    console.log(this.container_id + ' disconnected');
};

module.exports.Artemis = Artemis;
