'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

var Promise = require('bluebird');
var _ = require('lodash');
var Riak  = require('..');

var client = new Riak.Client();

var bucket = 'no-riak-test-search';
var schema = 'no-riak-test-schema';
var schemaContent = require('fs').readFileSync(require('path').resolve(__dirname, './search-schema.xml'), { encoding: 'utf8' });

describe('Yokozuna search API', function () {
    describe('schema and index', function () {
        var index = uniqueKey('no-riak-test-index');
        it('create schema', function () {
            return client.putSearchSchema({
                schema: {
                    name: schema,
                    content: schemaContent
                }
            });
        });

        it('get schema', function () {
            return client.getSearchSchema({
                name: schema
            })
            .then(function (result) {
                result.should.be.an('object');
                result.should.have.property('schema').that.is.an('object');
                result.schema.should.have.property('name', schema);
                result.schema.should.have.property('content', schemaContent);
            });
        });

        it('create index', function () {
            this.timeout(15000);
            return client.putSearchIndex({
                index: {
                    name: index,
                    schema: schema
                }
            });
        });

        it('get index', function () {
            return client.getSearchIndex({
                name: index
            })
            .then(function (result) {
                result.should.be.an('object').and.have.property('index');
                result.index.should.be.an('array').and.have.length(1);
                result.index[0].should.be.an('object');
                result.index[0].should.have.property('name', index);
                result.index[0].should.have.property('schema', schema);
            });
        });

        it('get index - all', function () {
            return client.getSearchIndex()
            .then(function (result) {
                result.should.be.an('object').and.have.property('index');
                result.index.should.be.an('array').and.have.length.at.least(1);
            });
        });

        it('delete index', function () {
            return client.delSearchIndex({
                name: index
            });
        });
    });

    describe('search', function () {
        var index = 'no-riak-test-index';
        before(function () {
            this.timeout(20000);
            return client.putSearchIndex({
                index: {
                    name: index,
                    schema: schema
                }
            })
            .then(function () {
                return client.setBucket({
                    bucket: bucket,
                    props: {
                        search_index: index
                    }
                });
            })
            .then(function () {
                return Promise.map(_.range(5), function (i) {
                    return client.put({
                        bucket: bucket,
                        key: 'key-' + i,
                        content: {
                            value: {
                                name: 'test',
                                value: 'v' + i + ' ' + 'qwe'
                            }
                        }
                    });
                });
            })
            .delay(2000);
        });

        after(function () {
            return client.setBucket({
                bucket: bucket,
                props: {
                    search_index: '_dont_index_'
                }
            });
        });

        it('search', function () {
            return client.search({
                q: 'name:test AND value:qwe',
                index: index
            })
            .then(function (result) {
                result.should.be.an('object');
                result.should.have.property('num_found', 5);
                result.should.have.property('max_score');
                result.should.have.property('docs').that.is.an('array');
                result.docs.should.have.length(5);
                _.find(result.docs, { _yz_rk: 'key-0' }).should.be.an('object').and.have.property('name', 'test');
                _.find(result.docs, { _yz_rk: 'key-1' }).should.be.an('object').and.have.property('name', 'test');
                _.find(result.docs, { _yz_rk: 'key-2' }).should.be.an('object').and.have.property('name', 'test');
                _.find(result.docs, { _yz_rk: 'key-3' }).should.be.an('object').and.have.property('name', 'test');
                _.find(result.docs, { _yz_rk: 'key-4' }).should.be.an('object').and.have.property('name', 'test');
            });
        });

        it('search - no results', function () {
            return client.search({
                q: 'name:not-found',
                index: index
            })
            .then(function (result) {
                result.should.be.an('object');
                result.should.have.property('num_found', 0);
                result.should.have.property('max_score');
                result.should.have.property('docs').that.is.an('array');
                result.docs.should.have.length(0);
            });
        });
    });
});
