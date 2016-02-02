'use strict';

var Promise    = require('bluebird');
var _          = require('lodash');

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

    self.waiting = []; // client requests waiting for free connection
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

    function _try() {
        return Promise.try(function () {
            var connection = self.connections.free.shift();

            if (connection === undefined) {
                if (self.connections.busy.length < self.options.max) {
                    connection = self.options.create();
                } else {
                    return new Promise(function (resolve) {
                        self.lastBursted = Date.now();
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
            _.remove(self.connections.busy, { id: _connection.id });
            // check if we can drop this connection:
            if ((self.connections.free.length >= self.options.min) && ((Date.now() - self.lastBursted) > self.options.idleTimeout)) {
                self.options.release(_connection);
            } else {
                self.connections.free.push(_connection);
            }
        }
    });
};

Pool.prototype.connection = function (handler) {
    return Promise.using(this._next(), handler);
};
