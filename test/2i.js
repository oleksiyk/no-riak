'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

var Promise = require('bluebird');
var Riak  = require('..');
var _       = require('lodash');

var bucket = 'no-riak-test-kv';
var client = new Riak.Client();

describe('Secondary indexes', function () {
    it('2i search, with max_results/continuation', function () {
        var keys = [], indexValue = uniqueKey('index');

        return Promise.all(_.range(3).map(function (i) {
            keys[i] = uniqueKey('key');
            return client.put({
                bucket: bucket,
                key: keys[i],
                content: {
                    value: 'i' + i,
                    indexes: [{
                        key: 'no-riak-test_bin',
                        value: indexValue
                    }]
                }
            });
        }))
        .then(function () {
            return client.index({
                bucket: bucket,
                index: 'no-riak-test_bin',
                qtype: 0,
                max_results: 2,
                key: indexValue
            });
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('results');
            result.results.should.be.an('array').and.have.length(2);
            result.should.have.property('continuation');
            result.continuation.should.be.a('string');

            return client.index({
                bucket: bucket,
                index: 'no-riak-test_bin',
                qtype: 0,
                max_results: 2,
                continuation: result.continuation,
                key: indexValue
            });
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('results');
            result.results.should.be.an('array').and.have.length(1);
            expect(result.continuation).to.eql(undefined);
        });
    });

    it('2i search all at once', function () {
        var keys = [], indexValue = uniqueKey('index');
        return Promise.all(_.range(3).map(function (i) {
            keys[i] = uniqueKey('key');
            return client.put({
                bucket: bucket,
                key: keys[i],
                content: {
                    value: 'i' + i,
                    indexes: [{
                        key: 'no-riak-test_bin',
                        value: indexValue
                    }]
                }
            });
        }))
        .then(function () {
            return client.index({
                bucket: bucket,
                index: 'no-riak-test_bin',
                qtype: 0,
                key: indexValue
            });
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('results');
            result.results.should.be.an('array').and.have.length(3);
            expect(result.continuation).to.eql(undefined);
        });
    });

    it('2i range query with return_terms=true', function () {
        var keys = [], indexValue = uniqueKey('index');
        return Promise.all(_.range(3).map(function (i) {
            keys[i] = uniqueKey('key');
            return client.put({
                bucket: bucket,
                key: keys[i],
                content: {
                    value: 'i' + i,
                    indexes: [{
                        key: 'no-riak-test_bin',
                        value: indexValue
                    }]
                }
            });
        }))
        .then(function () {
            return client.index({
                bucket: bucket,
                index: 'no-riak-test_bin',
                qtype: 1,
                return_terms: true,
                range_min: indexValue,
                range_max: indexValue
            });
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('results');
            result.results.should.be.an('array').and.have.length(3);
            expect(result.continuation).to.eql(undefined);
            result.results[0].should.be.an('object');
            result.results[0].should.have.property('key');
            result.results[0].should.have.property('value');
        });
    });

    it('2i invalid query', function () {
        return client.index({
            bucket: bucket,
            index: 'no-riak-test_bin',
            qtype: 0
        }).should.be.rejectedWith('Invalid equality query');
    });
});
