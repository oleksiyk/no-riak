'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

// var Promise = require('bluebird');
// var _ = require('lodash');
var Riak  = require('../..');
var client = new Riak.Client();

var bucket = 'no_riak_test_crdt_counter_bucket';
var bucketType = 'no_riak_test_crdt_counter';

describe('CRDT Counter', function () {
    it('should create new Counter', function () {
        var counter = new Riak.CRDT.Counter(client, {
            bucket: bucket,
            type: bucketType
        });

        counter.should.respondTo('load');
        counter.should.respondTo('save');
        counter.should.respondTo('increment');
        counter.should.respondTo('value');
        counter.should.respondTo('key');
    });

    it('increment by 1 if no value given', function () {
        var counter = new Riak.CRDT.Counter(client, {
            bucket: bucket,
            type: bucketType
        });

        counter.value().toNumber().should.be.eql(0);
        counter.increment().value().toNumber().should.be.eql(1);
    });

    it('increment with positive value', function () {
        var counter = new Riak.CRDT.Counter(client, {
            bucket: bucket,
            type: bucketType
        });

        counter.increment(5).value().toNumber().should.be.eql(5);
    });

    it('increment with negative value', function () {
        var counter = new Riak.CRDT.Counter(client, {
            bucket: bucket,
            type: bucketType
        });

        counter.increment(-5).value().toNumber().should.be.eql(-5);
    });

    it('new counter should be able to save/load', function () {
        var counter = new Riak.CRDT.Counter(client, {
            bucket: bucket,
            type: bucketType
        });

        expect(counter.key()).to.eql(undefined);

        return counter.increment(1).increment(-5).save().call('value')
        .then(function (v) {
            counter.key().should.be.a('string').and.have.length.gt(0);
            v.toNumber().should.be.eql(-4);
        });
    });

    it('should create new Counter with custom key', function () {
        var key = uniqueKey('counter');
        var counter = new Riak.CRDT.Counter(client, {
            bucket: bucket,
            type: bucketType,
            key: key
        });

        return counter.increment(2).increment(3).save()
        .then(function () {
            var _counter = new Riak.CRDT.Counter(client, {
                bucket: bucket,
                type: bucketType,
                key: key
            });

            return _counter.load().call('value');
        })
        .then(function (v) {
            v.toNumber().should.be.eql(5);
        });
    });

    it('should update existing counter', function () {
        var key = uniqueKey('counter');

        var counter = new Riak.CRDT.Counter(client, {
            bucket: bucket,
            type: bucketType,
            key: key
        });

        return counter
        .increment(1)
        .increment(2)
        .increment(-5)
        .save()
        .then(function () {
            return counter.increment(1).save();
        })
        .then(function () {
            var _counter = new Riak.CRDT.Counter(client, {
                bucket: bucket,
                type: bucketType,
                key: key
            });

            return _counter.increment(5).load().call('value')
            .then(function (v) {
                v.toNumber().should.be.eql(4);

                return _counter.save();
            });
        })
        .then(function () {
            return counter.load().call('value');
        })
        .then(function (v) {
            v.toNumber().should.be.eql(4);
        });
    });
});
