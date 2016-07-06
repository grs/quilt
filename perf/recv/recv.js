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

var args = require('yargs').env('RECV').options({
    'd': { alias: 'duration', default: 0, describe: 'length of time to run for, in seconds'},
    'a': { alias: 'address', default: 'examples', describe: 'address from which messages are received'},
    'h': { alias: 'host', default: default_host, describe: 'host to connect to'},
    'p': { alias: 'port', default: default_port, describe: 'port to connect to'},
    'l': { alias: 'listen', default: 9090, describe: 'port to listen on for prometheus scraping'}
}).help('help').argv;

var disconnects = new prometheus.Counter('disconnects', 'The number of times the receiver was disconnected', ['address']);
var received = new prometheus.Counter('messages_received', 'The number of messages received', ['address']);
var latency = new prometheus.Histogram('message_latency_millis', 'The latency in milliseconds', ['address'], {buckets:[1, 5, 10, 20, 30, 50, 100, 250]});

container.on('message', function (context) {
    received.labels(args.address).inc();
    if (context.message.timestamp) {
        latency.labels(args.address).observe(Date.now() - context.message.timestamp);
    }
});
container.on('disconnected', function (context) {
    disconnects.labels(args.address).inc();
});

var conn = container.connect({'port':args.port});
conn.open_receiver(args.address);

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
