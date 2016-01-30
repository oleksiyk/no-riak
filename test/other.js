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
});
