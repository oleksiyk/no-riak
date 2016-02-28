'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

var Promise = require('bluebird');
var _ = require('lodash');
var Riak  = require('..');

var bucket = 'no-riak-test-kv';

describe('Server connections', function () {
    it('pool should split connections according to server weights', function () {
        var client = new Riak.Client({
            connectionString: '10.0.1.1:8087:4,10.0.1.2:8087:3,10.0.1.3:8087:2',
            pool: {
                min: 9
            }
        });

        // not good to look at internals, but..
        client.pool.connections.free.length.should.be.eql(9);

        client.pool.count().should.be.eql({
            free: {
                '10.0.1.1:8087': 4,
                '10.0.1.2:8087': 3,
                '10.0.1.3:8087': 2
            },
            busy: {}
        });
    });

    it('failed connection should be rejected with RiakConnectionError (retries=0)', function () {
        var client = new Riak.Client({
            connectionString: '127.1.2.3:8087',
            connectionTimeout: 100,
            retries: 0
        });

        return client.init().catch(function (err) {
            err.toString().should.include('127.1.2.3');
            err.toString().should.include('8087');

            err.toJSON().should.be.an('object');
            err.toJSON().should.have.property('name', 'RiakConnectionError');
            err.toJSON().should.have.property('server', '127.1.2.3:8087');
        });
    });

    it('failed connection should be rejected with "no more connections" (retries > maxConnectionErrors)', function () {
        var client = new Riak.Client({
            connectionString: '127.1.2.3:8087',
            connectionTimeout: 100,
            retries: 3,
            maxConnectionErrors: 3
        });

        return client.init().should.eventually.be.rejectedWith('No Riak connections available: all hosts down');
    });

    it('should throw No Connections when all servers disabled', function () {
        var client = new Riak.Client({
            connectionString: '127.1.2.3:8087:10, 127.1.2.4:8087:1',
            connectionTimeout: 100,
            maxConnectionErrors: 1,
            retries: 3
        });

        return client.ping().should.be.rejectedWith('Error: No Riak connections available: all hosts down');
    });

    it('should create up to max connections and put new requests in waiting queue', function () {
        var client = new Riak.Client({
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
        var client = new Riak.Client({
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

    it('Connection should be able to grow its buffer', function () {
        var client = new Riak.Client({
            connectionBufSize: 10 // 10 bytes
        });

        var data = new Buffer(100000);

        client.pool.connections.free[0].buffer.length.should.be.eql(10);

        return client.put({
            bucket: bucket,
            content: {
                value: data
            }
        })
        .then(function (result) {
            return client.get({
                bucket: bucket,
                key: result.key
            });
        })
        .then(function (result) {
            result.should.be.an('object');
            result.should.have.property('content').that.is.an('array');
            result.content.should.have.length(1);
            result.content[0].should.have.property('value');
            result.content[0].value.should.be.eql(data);
        });
    });

    it('should disable failed server', function () {
        var client = new Riak.Client({
            connectionString: '127.1.2.3:8087:10,127.0.0.1:8087:1',
            connectionTimeout: 100,
            maxConnectionErrors: 3,
            maxConnectionErrorsPeriod: 60 * 1000,
            retries: 0,
            pool: {
                min: 9
            }
        });

        // 3 failed requests and this connection should be closed
        return Promise.map([0, 1, 2], function () {
            return client.ping().catch(function (err) {
                err.should.be.an.instanceOf(Riak.RiakConnectionError);
            });
        })
        .then(function () {
            client.pool.count().should.be.eql({
                free: { '127.0.0.1:8087': 2 },
                busy: {}
            });
        });
    });

    it('client.end() should return array of closed connections', function () {
        var client = new Riak.Client({
            connectionString: '127.1.2.3:8087:10, 127.1.2.4:8087:1',
            pool: {
                min: 5
            },
            connectionTimeout: 100
        });

        client.end().then(function (result) {
            result.should.be.an('array').and.have.length(5);
        });
    });

    it('Client should now allow wrong connectionString', function () {
        function _createClient() {
            var client = new Riak.Client({ // eslint-disable-line
                connectionString: 'a,b,c',
            });
        }

        expect(_createClient).to.throw('No Riak servers were defined');
    });
});
