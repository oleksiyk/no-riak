'use strict';

var Promise    = require('bluebird');
var _          = require('lodash');
var errors     = require('./errors');

function Pool(options) {
    var self = this;

    self.options = _.defaultsDeep(options || {}, {
        min: 5,
        max: 20, // make sure protobuf.backlog is set to handle this max value (riak.conf)
        idleTimeout: 60 * 1000,
        create: function () { throw new Error('Pool missing create callback'); },
        release: function () { throw new Error('Pool missing release callback'); }
    });

    self.connections = {
        free: [],
        busy: []
    };

    self.waiting = []; // clients waiting for free connection
    self.lastBursted = null;

    _.range(self.options.min).map(function () {
        self.connections.free.push(self.options.create());
    });
}

module.exports = Pool;

/**
 * Get next free connection
 */
Pool.prototype._next = function () {
    var self = this;

    return Promise.try(function () {
        var connection = self.connections.free.shift();

        if (connection === undefined) {
            if (self.connections.busy.length < self.options.max) {
                connection = self.options.create();
            } else {
                return new Promise(function (resolve, reject) {
                    self.lastBursted = Date.now();
                    self.waiting.push({
                        resolve: resolve,
                        reject: reject
                    });
                });
            }
        }

        if (connection) {
            self.connections.busy.push(connection);
        }

        return connection;
    })
    .tap(function (connection) {
        var err;
        if (!connection) { // .create() returned null
            err = new Error('No connections available');
            self.waiting.forEach(function (_w) {
                _w.reject(err);
            });
            self.waiting = [];
            throw err;
        }
    })
    .disposer(function (connection, promise) {
        var w = self.waiting.shift();

        if (promise.isRejected() && (promise.reason() instanceof errors.ConnectionError)) {
            _.remove(self.connections.busy, { id: connection.id });
            self.options.release(promise.reason(), connection);

            // close all free connections to the same server
            _.remove(self.connections.free, function (c) {
                if (c.host() === connection.host() && c.port() === connection.port()) {
                    self.options.release(null, c);
                    return true;
                }
            });
            connection = self.options.create();
        }

        if (w !== undefined) {
            w.resolve(connection); // even if connection=null it will passed to .tap and correctly handled there, closing the waiting queue
        } else if (connection) {
            _.remove(self.connections.busy, { id: connection.id });
            // check if we can drop this connection:
            if ((self.connections.free.length >= self.options.min) && ((Date.now() - self.lastBursted) > self.options.idleTimeout)) {
                self.options.release(null, connection);
            } else {
                self.connections.free.push(connection);
            }
        }
    });
};

Pool.prototype.connection = function (handler) {
    return Promise.using(this._next(), handler);
};
