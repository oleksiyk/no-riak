'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

// var Promise = require('bluebird');
// var _ = require('lodash');
var exec   = require('child_process').exec;
var Client = require('..');

// these are the only ciphers enabled in riak:
var ciphers = 'DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA256:DHE-RSA-AES256-SHA:AES128-SHA256:AES128-SHA:AES256-SHA256:AES256-SHA:RC4-SHA';

describe('Authentication and TLS', function () {
    after(function (done) {
        exec('make disable-security', { env: process.env }, done);
    });

    describe('Security disabled', function () {
        it('returns error when trying to start TLS', function () {
            var client = new Client({
                auth: {
                    user: 'no-riak',
                    password: 'secret'
                }
            });

            return client.ping().should.be.rejectedWith(Client.RiakError, 'Security not enabled; STARTTLS not allowed');
        });
    });

    describe('Security enabled', function () {
        before(function (done) {
            exec('make enable-security', { env: process.env }, done);
        });

        it('should start TLS and authenticate', function () {
            var client = new Client({
                auth: {
                    user: 'no-riak',
                    password: 'secret'
                },
                tls: {
                    // secureProtocol: 'SSLv23_method',
                    ciphers: ciphers
                }
            });

            return client.ping();
        });

        it('should not allow wrong credentials', function () {
            var client = new Client({
                auth: {
                    user: 'no-riak-wrong',
                    password: 'secret-wrong'
                },
                tls: {
                    ciphers: ciphers
                }
            });

            this.timeout(10000);

            return client.ping().should.be.rejectedWith(Client.RiakError, 'Authentication failed');
        });

        it('should not allow missing credentials', function () {
            var client = new Client({
                tls: {
                    ciphers: ciphers
                }
            });

            this.timeout(10000);

            return client.ping().should.be.rejectedWith(Client.RiakError, 'Security is enabled, please STARTTLS first');
        });
    });
});
