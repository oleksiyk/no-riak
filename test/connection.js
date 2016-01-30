'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

var Promise = require('bluebird');
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
            return c.options.host + ':' + c.options.port;
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

        return client.init().should.be.rejectedWith('Connection timeout to 127.1.2.3:8087');
    });

    it('should create up to max connections and put new requests in waiting queue', function () {
        var client = new Client({
            pool: {
                min: 5,
                max: 20
            }
        });

        // send 30 concurrent requests
        return Promise.map(_.range(30), function () {
            return client.getServerInfo();
        })
        .then(function (results) {
            results.should.be.an('array').and.have.length(30);

            // pool should have created 15 new connections, 20 in total
            client.pool.connections.free.length.should.be.eql(20);
        });
    });

    it('should close excessive connections after the burst', function () {
        var client = new Client({
            pool: {
                min: 5,
                max: 20,
                idleTimeout: 100
            }
        });

        // send 30 concurrent requests
        return Promise.map(_.range(30), function () {
            return client.getServerInfo();
        })
        .then(function (results) {
            results.should.be.an('array').and.have.length(30);
            // pool should have created 15 new connections, 20 in total
            client.pool.connections.free.length.should.be.eql(20);
        })
        .delay(200)
        .then(function () {
            // pool will close one connection at time after each request is finished
            // send requests serially to avoid another burst
            return Promise.each(_.range(30), function () {
                return client.getServerInfo();
            });
        })
        .then(function () {
            client.pool.connections.free.length.should.be.eql(5);
        });
    });
});
