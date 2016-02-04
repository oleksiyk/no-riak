'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

// var Promise = require('bluebird');
// var _ = require('lodash');
var exec   = require('child_process').exec;
var Riak = require('..');

var auth = {
    user: 'no-riak',
    password: 'secret'
};

var bucket = 'no-riak-test-kv';

describe('Authentication and TLS', function () {
    after(function (done) {
        exec('make disable-security', { env: process.env }, done);
    });

    describe('Security disabled', function () {
        it('returns error when trying to start TLS', function () {
            var client = new Riak.Client({
                auth: auth
            });

            return client.ping().should.be.rejectedWith(Riak.RiakError, 'Security not enabled; STARTTLS not allowed');
        });
    });

    describe('Security enabled', function () {
        before(function (done) {
            this.timeout(4000);
            exec('make enable-security', { env: process.env }, done);
        });

        it('should start TLS and authenticate', function () {
            var client = new Riak.Client({
                auth: auth
            });

            return client.ping();
        });

        it('should not allow wrong credentials', function () {
            var client = new Riak.Client({
                auth: {
                    user: 'no-riak-wrong',
                    password: 'secret-wrong'
                }
            });

            this.timeout(10000);

            return client.ping().should.be.rejectedWith(Riak.RiakError, 'Authentication failed');
        });

        it('should not allow missing credentials', function () {
            var client = new Riak.Client();

            this.timeout(10000);

            return client.ping().should.be.rejectedWith(Riak.RiakError, 'Security is enabled, please STARTTLS first');
        });

        it('should allow put and get', function () {
            var client = new Riak.Client({
                auth: auth
            });

            return client.put({
                bucket: bucket,
                content: {
                    value: 'hello'
                }
            })
            .then(function (result) {
                return client.get({
                    bucket: bucket,
                    key: result.key
                });
            });
        });

        it('should not allow delete', function () {
            var client = new Riak.Client({
                auth: auth
            });

            return client.put({
                bucket: bucket,
                content: {
                    value: 'hello'
                }
            })
            .then(function (result) {
                return client.del({
                    bucket: bucket,
                    key: result.key
                });
            })
            .should.be.rejectedWith(Riak.RiakError, 'riak_kv.delete');
        });
    });
});
