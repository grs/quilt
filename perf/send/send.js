/*
 * Copyright 2015 Red Hat Inc.
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
var container = require('rhea');
var prometheus = require('prom-client');
var express = require('express');

var default_host = process.env.MESSAGING_SERVICE_HOST || '127.0.0.1';
var default_port = process.env.MESSAGING_SERVICE_PORT || 5672;

var args = require('yargs').env('SEND').options({
    'd': { alias: 'duration', default: 0, describe: 'length of time to run for, in seconds'},
    'w': { alias: 'window', default: 10, describe: 'maximum number of in-doubt messages that can be outstanding'},
    's': { alias: 'size', default: 32, describe: 'message body size in bytes'},
    'a': { alias: 'address', default: 'examples', describe: 'address to which messages are sent'},
    'h': { alias: 'host', default: default_host, describe: 'host to connect to'},
    'p': { alias: 'port', default: default_port, describe: 'port to connect to'},
    'l': { alias: 'listen', default: 9090, describe: 'port to listen on for prometheus scraping'}
}).help('help').argv;

var disconnects = new prometheus.Counter('disconnects', 'The number of times the sender was disconnected', ['address']);
var sent = new prometheus.Counter('messages_sent', 'The number of messages sent', ['address']);
var accepted = new prometheus.Counter('messages_accepted', 'The number of messages accepted by peer', ['address']);
var released = new prometheus.Counter('messages_released', 'The number of messages released by peer', ['address']);
var rejected = new prometheus.Counter('messages_rejected', 'The number of messages rejected by peer', ['address']);
var indoubt = new prometheus.Gauge('messages_unacked', 'The number of messages sent but not yet acknowledged', ['address']);
var sequence = 0;
var window = 0;
var msg_body = Array(args.size+1).join('x');

function send(sender) {
    while (sender.connection.is_open() && sender.sendable() && window < args.window) {
        ++window;
        sent.labels(args.address).inc();
        indoubt.labels(args.address).inc();
        sender.send({id: ++sequence, timestamp:Date.now(), body:msg_body})
    }
}

container.on('sendable', function (context) {
    send(context.sender);
});
container.on('accepted', function (context) {
    accepted.labels(args.address).inc();
});
container.on('released', function (context) {
    released.labels(args.address).inc();
});
container.on('rejected', function (context) {
    rejected.labels(args.address).inc();
});
container.on('settled', function (context) {
    var do_send = window === args.window;
    --window;
    indoubt.labels(args.address).dec();
    if (do_send) send(context.sender);
});
container.on('disconnected', function (context) {
    disconnects.labels(args.address).inc();
});

var conn = container.connect({port:args.port, host:args.host});
conn.open_sender(args.address);

var server;
if (args.listen) {
    var app = express();
    app.get('/metrics', function(req, res) {
        res.end(prometheus.register.metrics());
    });
    server = app.listen(args.listen);
}

function close() {
    conn.close();
    if (server) server.close();
}

if (args.duration) {
    setTimeout(close, args.duration*1000);
}
