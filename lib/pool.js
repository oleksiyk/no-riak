'use strict';

var Promise    = require('bluebird');
var Connection = require('./connection');
var _          = require('lodash');
var WRRPool    = require('wrr-pool');
var Protocol   = require('./protocol');

var multipleResponse = {
    RpbListKeysReq: true,
    RpbListBucketsReq: true,
    RpbMapRedReq: true,
    RpbIndexReq: true
};

function Pool(connectionString, options) {
    var self = this;

    self.options = _.defaultsDeep(options || {}, {
        min: 5,
        max: 20 // make sure protobuf.backlog is set to handle this max value (riak.conf)
    });

    self.protocol = new Protocol({
        bufferSize: 256 * 1024,
        resultCopy: true
    });

    self.servers = new WRRPool(); // weighted round robin pool

    self.connections = {
        free: [],
        busy: []
    };
    self.waiting = [];

    connectionString.split(',').map(function (hostStr) {
        var h = hostStr.trim().split(':');

        self.servers.add({
            host   : h[0],
            port   : parseInt(h[1])
        }, parseInt(h[2] || 10));
    });

    _.range(self.options.min).map(function () {
        var server = self.servers.next();

        self.connections.free.push(new Connection({
            host: server.host,
            port: server.port
        }));
    });
}

module.exports = Pool;

/**
 * Process raw buffer received from Riak
 */
Pool.prototype._processTask = function (task, data) {
    var self = this, result, done;

    try {
        result = self.protocol.read(data).Response().result;
    } catch (err) {
        task.reject(err);
    }

    if (!task.multiple) {
        task.resolve(result);
    } else {
        done = result.done;
        delete result.done;

        if (!_.isEmpty(result)) {
            task.result.push(result);
        }

        if (done) {
            task.resolve(task.result);
        }
    }
};

/**
 * Get next free connection
 */
Pool.prototype._next = function () {
    var self = this;

    function _try() {
        return Promise.try(function () {
            var connection = self.connections.free.shift(), server;

            if (connection === undefined) {
                if (self.connections.busy.length < self.options.max) {
                    server = self.servers.next();
                    connection = new Connection({
                        host: server.host,
                        port: server.port
                    });
                } else {
                    return new Promise(function (resolve) {
                        self.waiting.push(resolve);
                    });
                }
            }

            self.connections.busy.push(connection);

            return connection;
        });
    }

    return _try().disposer(function (_connection) {
        var w = self.waiting.shift();

        if (w !== undefined) {
            w(_connection);
        } else {
            self.connections.free.push(_connection);
            _.remove(self.connections.busy, { id: _connection.id });
        }
    });
};

/**
 * Send request to Riak
 * @param  {String} request Riak PB message name
 * @param  {Obkect} params  message params
 * @return {Promise}
 */
Pool.prototype.send = function (request, params) {
    var self = this, buffer;

    return Promise.using(self._next(), function (connection) {
        return new Promise(function (resolve, reject) {
            var task = {
                result: [],
                multiple: multipleResponse[request] || false,
                resolve: resolve,
                reject: reject
            };

            task.process = _.bind(self._processTask, self, task);

            buffer = self.protocol.write().Request(request, params).result;

            connection.send(buffer, task).catch(function (err) {
                reject(err);
            });
        });
    });
};
