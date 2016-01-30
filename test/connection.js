'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

// var Promise = require('bluebird');
var _ = require('lodash');
var Client  = require('..');

describe('Server connections', function () {
    it('pool should split connections according to server weights', function () {
        var client = new Client({
            connectionString: '10.0.1.1:8087:4,10.0.1.2:8087:3,10.0.1.3:8087:2',
            pool: {
                min: 9
            }
        });

        // not good to look at internals, but..
        client.pool.connections.free.length.should.be.eql(9);

        _.countBy(client.pool.connections.free, function (c) {
            return c.host + ':' + c.port;
        }).should.be.eql({
            '10.0.1.1:8087': 4,
            '10.0.1.2:8087': 3,
            '10.0.1.3:8087': 2
        });
    });

    it('failed connection should be rejected with ConnectionError', function () {
        var client = new Client({
            connectionString: '127.1.2.3:8087',
            pool: {
                connectionTimeout: 100
            }
        });

        return client.init().should.be.rejectedWith('Connection timeout');
    });
});
