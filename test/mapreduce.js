'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

var Promise = require('bluebird');
var Client  = require('..');
var _       = require('lodash');

var bucket = 'no-riak-test-kv';
var client = new Client();

describe('Map/Reduce', function () {
    it('map/reduce', function () {
        var keys = [];
        return Promise.all(_.range(3).map(function (i) {
            keys[i] = uniqueKey('key');
            return client.put({
                bucket: bucket,
                key: keys[i],
                content: {
                    value: {
                        num: i + 10
                    }
                }
            });
        }))
        .then(function () {
            return client.mapReduce({
                request: {
                    inputs: _.map(keys, function (k) { return [bucket, k]; }),
                    query: [{
                        map: { // this phase will return JSON decoded values for each input
                            source: 'function(v) { var d = Riak.mapValuesJson(v)[0]; return [d]; }',
                            language: 'javascript',
                            keep: true
                        }
                    }, { // this phase will return the `num` property of each value
                        reduce: {
                            source: 'function(values) { return values.map(function(v){ return v.num; }); }',
                            language: 'javascript',
                            keep: true
                        }
                    }, { // this phase will return a sum of all values
                        reduce: {
                            module: 'riak_kv_mapreduce',
                            function: 'reduce_sum',
                            language: 'erlang',
                            keep: true
                        }
                    }]
                }
            });
        })
        .then(function (results) {
            results.should.be.an('array').and.have.length(3); // each index in array is (phase number - 1), even if phase.keep was false

            results[0].should.be.an('array').and.have.length(3); // array of 1st phase results
            results[0].should.include({ num: 10 });

            results[1].should.be.an('array').and.have.length(3); // array of 2nd phase results
            results[1].should.include(10);

            results[2].should.be.an('array').and.have.length(1); // array of 3rd phase results
            results[2][0].should.be.eql(10 + 11 + 12);
        });
    });

    it('map/reduce - keep=false', function () {
        var keys = [];
        return Promise.all(_.range(3).map(function (i) {
            keys[i] = uniqueKey('key');
            return client.put({
                bucket: bucket,
                key: keys[i],
                content: {
                    value: {
                        num: i + 10
                    }
                }
            });
        }))
        .then(function () {
            return client.mapReduce({
                request: {
                    inputs: _.map(keys, function (k) { return [bucket, k]; }),
                    query: [{
                        map: {
                            source: 'function(v) { var d = Riak.mapValuesJson(v)[0]; return [d]; }',
                            language: 'javascript'
                        }
                    }, {
                        reduce: {
                            source: 'function(values) { return values.map(function(v){ return v.num; }); }',
                            language: 'javascript'
                        }
                    }, {
                        reduce: {
                            module: 'riak_kv_mapreduce',
                            function: 'reduce_sum',
                            language: 'erlang'
                        }
                    }]
                }
            });
        })
        .then(function (results) {
            results.should.be.an('array').and.have.length(3); // each index in array is (phase number - 1), even if phase.keep was false

            expect(results[0]).to.eql(undefined);
            expect(results[1]).to.eql(undefined);

            results[2].should.be.an('array').and.have.length(1); // array of 3rd phase results
            results[2][0].should.be.eql(10 + 11 + 12);
        });
    });
});
