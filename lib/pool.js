'use strict';

// var Promise    = require('bluebird');
var Connection = require('./connection');
var _          = require('lodash');
var WRRPool    = require('wrr-pool');

function Pool(connectionString, options) {
    var self = this;

    self.options = _.defaultsDeep(options || {}, {
        connections: 50
    });

    self.servers = new WRRPool();

    self.connections = new WRRPool();

    connectionString.split(',').map(function (hostStr) {
        var h = hostStr.trim().split(':');

        self.servers.add({
            host   : h[0],
            port   : parseInt(h[1])
        }, parseInt(h[2] || 10));
    });

    _.range(0, self.options.connections).map(function () {
        var server = self.servers.next();
        self.connections.add(new Connection({
            host: server.host,
            port: server.port
        }), 10);
    });
}

module.exports = Pool;

Pool.prototype.send = function (buffer) {
    var self = this;

    return self.connections.next().send(buffer);
};
