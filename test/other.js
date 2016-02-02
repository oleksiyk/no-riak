'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

// var Promise = require('bluebird');
// var _ = require('lodash');
var Client  = require('..');

describe('Other', function () {
    it('getServerInfo', function () {
        var client = new Client();

        return client.getServerInfo()
        .then(function (result) {
            result.should.be.an('object');
            result.should.have.property('node').that.is.a('string');
            result.should.have.property('server_version').that.is.a('string');
        });
    });

    it('ping', function () {
        var client = new Client();

        return client.ping()
        .then(function (result) {
            expect(result).to.eql(null);
        });
    });

    it('ping - unreachable host', function () {
        var client = new Client({
            connectionString: '127.1.2.3:8087',
            pool: {
                connectionTimeout: 200
            }
        });

        return client.ping().should.be.rejectedWith(Client.ConnectionError);
    });

    it('RiakError', function () {
        var client = new Client();

        return client.index({
            bucket: uniqueKey('bucket'),
            index: uniqueKey('index'),
            qtype: 0
        })
        .catch(function (err) {
            err.should.be.instanceOf(Client.RiakError);
            err.toString().should.include('RiakError');
            err.toJSON().should.be.an('object');
            err.toJSON().should.have.property('name', 'RiakError');
            // errcode is always 0, see https://github.com/basho/riak_kv/issues/336
            err.toJSON().should.have.property('code').that.is.a('number');
            err.toJSON().should.have.property('message').that.is.a('string');
        });
    });
});
