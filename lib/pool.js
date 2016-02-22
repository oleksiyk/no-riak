'use strict';

var Promise             = require('bluebird');
var _                   = require('lodash');
var RiakConnectionError = require('./errors').RiakConnectionError;

function Pool(options) {
    var self = this;

    /* istanbul ignore next */
    self.options = _.defaultsDeep(options || {}, {
        min: 5,
        max: 20, // make sure protobuf.backlog is set to handle this max value (riak.conf)
        idleTimeout: 60 * 1000,
        create: function () { throw new Error('Pool missing create callback'); },
        release: function () { throw new Error('Pool missing release callback'); },
        check: function () { throw new Error('Pool missing check callback'); }
    });

    self.connections = {
        free: [],
        busy: []
    };

    self.waiting = []; // clients waiting for free connection
    self.lastBursted = null;

    _.range(self.options.min).forEach(function () {
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
            self.lastBursted = Date.now();
            if (self.connections.busy.length < self.options.max) {
                connection = self.options.create();
            } else {
                return new Promise(function (resolve, reject) {
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
        if (!connection) { // .create() or .check() returned null
            err = new RiakConnectionError(null, 'No connections available');
            self.waiting.forEach(function (_w) {
                _w.reject(err);
            });
            self.waiting = [];
            throw err;
        }
    })
    .disposer(function (connection, promise) {
        var w = self.waiting.shift(),
            err = promise.isRejected() ? promise.reason() : null;

        _.remove(self.connections.busy, { id: connection.id });

        // optionally replace failed connection
        connection = self.options.check(err, connection);

        if (w !== undefined) {
            w.resolve(connection); // even if connection is null it will be passed to .tap and correctly handled there, closing the waiting queue
            if (connection) {
                self.connections.busy.push(connection);
            }
        } else if (connection) {
            // release excessive connection
            if ((self.connections.free.length >= self.options.min) && ((Date.now() - self.lastBursted) > self.options.idleTimeout)) {
                self.options.release(connection);
            } else {
                self.connections.free.push(connection);
            }
        }
    });
};

Pool.prototype.connection = function (handler) {
    return Promise.using(this._next(), handler);
};

Pool.prototype._close = function (which, predicate) {
    var self = this;

    if (typeof predicate !== 'function') {
        predicate = function () { return true; };
    }

    return _.remove(self.connections[which], function (c) {
        if (predicate(c) === true) {
            self.options.release(c);
            return true;
        }
        return false;
    });
};

Pool.prototype.close = function (which, predicate) {
    if (which === 'all' || which === undefined) {
        return Array.prototype.concat(
            this._close('free', predicate),
            this._close('busy', predicate)
        );
    }
    return this._close(which, predicate);
};

Pool.prototype.count = function () {
    var self = this;

    return _.mapValues(self.connections, function (_v, which) {
        return _.countBy(self.connections[which], function (c) {
            return c.server();
        });
    });
};
