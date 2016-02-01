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

var routers = {};

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
        this.create_entity('connector', {name:'key', role:'inter-router', addr:parts[0], port:parts[1], idleTimeoutSeconds:0});
    }
};

Router.prototype.init = function (context) {
    this.address = context.receiver.remote.attach.source.address;
    this.query('listener', {attributeNames:['identity', 'name', 'addr', 'port', 'role']});
};

Router.prototype.request = function (id, operation, properties, body) {
    if (this.sender) {
        var req = {properties:{reply_to:this.address, correlation_id:id}};
        req.application_properties = properties || {};
        req.application_properties.operation = operation;
        req.body = body;
        this.sender.send(req);
    }
};

Router.prototype.query = function (type, options) {
    this.request('QUERY-' + type, 'QUERY', {entityType:type}, options || {attributeNames:[]});
};

Router.prototype.create_entity = function (type, attributes) {
    this.request('CREATE-' + type, 'CREATE', {'type':type}, attributes || {});
};

// e.g. this.delete_entity({type:'connector', name:'router-b'});
Router.prototype.delete_entity = function (identifier) {
    this.request('DELETE-' + type, 'DELETE', identifier, {});
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

Router.prototype.incoming = function (context) {
    //console.log('Got message: ' + JSON.stringify(context.message));
    if (context.message.properties.correlation_id === 'QUERY-connector') {
        foreach_result(context.message.body, this._process_connector.bind(this));
        console.log('Updated connectors for ' + this.listener + ': ' + JSON.stringify(this.connected));
        check_full_mesh();
    } else if (context.message.properties.correlation_id === 'QUERY-listener') {
        var listeners = [];
        foreach_result(context.message.body, function (record) { if (record.role === 'inter-router') listeners.push(record.addr + ':' + record.port)});
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
    } else if (context.message.properties.correlation_id === 'CREATE-connector' || context.message.properties.correlation_id === 'DELETE-connector') {
        if (context.message.application_properties.statusDescription === 'Created') {
            console.log('Connector creation succeeded, requerying...');
            //requery
            this.query('connector', {attributeNames:['identity', 'name', 'addr', 'port', 'role']});
        } else {
            console.log('ERROR: ' + JSON.stringify(context.message));
        }
    } else {
        console.log('ERROR: unexpected correlation_id \'' + context.message.properties.correlation_id + '\' for ' + JSON.stringify(context.message));
    }
};


amqp.on('connection_open', function(context) {
    //verify that it is a dispatch router, else disconnect for now...
    if (context.connection.remote.open.properties && context.connection.remote.open.properties.product === 'qpid-dispatch-router') {
        var r = new Router(context.connection.remote.open.container_id, context.connection);
        console.log('Router connected from ' + context.connection.remote.open.container_id);
        context.connection.on('connection_close', function(context) {
            //TODO: delete after some time to allow for reconnection
            //delete routers[id];
        });
        context.connection.on('disconnect', function(context) {
            //TODO: delete after some time to allow for reconnection
            //delete routers[id];
        });
    } else {
        console.log('Closing connection for unrecognised client');
        context.connection.local.close.error = {condition:'amqp:connection:forced', description:'At present this only works with qpid dispatch routers'};
        context.connection.close();
    }
});

amqp.sasl_server_mechanisms.enable_anonymous();
amqp.listen({port:55672});
