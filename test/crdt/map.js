'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

// var Promise = require('bluebird');
// var _ = require('lodash');
var Client  = require('../..');
var client = new Client();

var bucket = 'no_riak_test_crdt_map_bucket';
var bucketType = 'no_riak_test_crdt_map';

describe('CRDT Map', function () {
    it('should create new Map', function () {
        var map = new Client.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        map.should.respondTo('load');
        map.should.respondTo('save');
        map.should.respondTo('update');
        map.should.respondTo('remove');
        map.should.respondTo('value');
    });

    it('add new field', function () {
        var map = new Client.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        return map
        .update('key1', new Client.CRDT.Counter().increment(5))
        .value()
        .then(function (v) {
            v.should.be.an('object');
            v.should.have.property('key1');
            v.key1.toNumber().should.be.eql(5);
        });
    });

    it('add multiple fields', function () {
        var map = new Client.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        return map
        .update('key1', new Client.CRDT.Counter().increment(-5))
        .update('key2', new Client.CRDT.Set().add('a1', 'a2', 'a3').remove('a2'))
        .value()
        .then(function (v) {
            v.should.be.an('object');
            v.should.have.property('key1');
            v.should.have.property('key2');
            v.key1.toNumber().should.be.eql(-5);
            v.key2.should.be.eql(['a1', 'a3']);
        });
    });

    it('should save/load', function () {
        var map = new Client.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        return map
        .update('key1', new Client.CRDT.Counter().increment(-5))
        .update('key2', new Client.CRDT.Set().add('a1', 'a2', 'a3').remove('a2'))
        .save()
        .call('value')
        .then(function (v) {
            map.key().should.be.a('string').and.have.length.gt(0);
            v.should.be.an('object');
            v.should.have.property('key1');
            v.should.have.property('key2');
            v.key1.toNumber().should.be.eql(-5);
            v.key2.should.be.eql(['a1', 'a3']);
        });
    });

    it('remove field before save', function () {
        var map = new Client.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        return map
        .update('key1', new Client.CRDT.Counter().increment(-5))
        .update('key2', new Client.CRDT.Set().add('a1', 'a2', 'a3').remove('a2'))
        .remove('key1', Client.CRDT.Counter)
        .save()
        .call('value')
        .then(function (v) {
            v.should.be.an('object');
            v.should.not.have.property('key1');
            v.should.have.property('key2');
            v.key2.should.be.eql(['a1', 'a3']);
        });
    });

    it('remove field after save', function () {
        var map = new Client.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        return map
        .update('key1', new Client.CRDT.Counter().increment(-5))
        .update('key2', new Client.CRDT.Set().add('a1', 'a2', 'a3').remove('a2'))
        .save()
        .then(function () {
            return map.remove('key1', Client.CRDT.Counter).save();
        })
        .call('value')
        .then(function (v) {
            v.should.be.an('object');
            v.should.not.have.property('key1');
            v.should.have.property('key2');
            v.key2.should.be.eql(['a1', 'a3']);
        });
    });

    it('update same field with different type on new object', function () {
        var map = new Client.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        return map
        .update('key1', new Client.CRDT.Counter().increment(-5))
        .update('key1', new Client.CRDT.Set().add('a1', 'a2', 'a3'))
        .save()
        .call('value')
        .then(function (v) {
            v.should.be.an('object');
            v.should.have.property('key1');
            v.key1.should.be.eql(['a1', 'a2', 'a3']);
        });
    });

    it('update same field with different type on saved object', function () {
        var map = new Client.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        return map
        .update('key1', new Client.CRDT.Counter().increment(-5))
        .save()
        .then(function () {
            return map.update('key1', new Client.CRDT.Set().add('a1', 'a2', 'a3')).save();
        })
        .call('value')
        .then(function (v) {
            v.should.be.an('object');
            v.should.have.property('key1');
            v.key1.should.be.eql(['a1', 'a2', 'a3']);
        });
    });
});
