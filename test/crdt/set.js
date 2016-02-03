'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

// var Promise = require('bluebird');
// var _ = require('lodash');
var Client  = require('../..');
var client = new Client();

var bucket = 'no_riak_test_crdt_set_bucket';
var bucketType = 'no_riak_test_crdt_set';

describe('CRDT Set', function () {
    it('should create new Set', function () {
        var set = new Client.CRDT.Set(client, {
            bucket: bucket,
            type: bucketType
        });

        set.should.respondTo('load');
        set.should.respondTo('save');
        set.should.respondTo('add');
        set.should.respondTo('remove');
        set.should.respondTo('value');
    });

    it('add single value to set', function () {
        var set = new Client.CRDT.Set(client, {
            bucket: bucket,
            type: bucketType
        });

        return set.add('a1').value()
        .then(function (v) {
            v.should.be.eql(['a1']);
        });
    });

    it('add multiple values to set', function () {
        var set = new Client.CRDT.Set(client, {
            bucket: bucket,
            type: bucketType
        });

        return set.add('a1', 'a2', 'a3').value()
        .then(function (v) {
            v.should.be.eql(['a1', 'a2', 'a3']);
        });
    });

    it('remove single value from set', function () {
        var set = new Client.CRDT.Set(client, {
            bucket: bucket,
            type: bucketType
        });

        return set.add('a1', 'a2', 'a3').remove('a2').value()
        .then(function (v) {
            v.should.be.eql(['a1', 'a3']);
        });
    });

    it('remove multiple values from set', function () {
        var set = new Client.CRDT.Set(client, {
            bucket: bucket,
            type: bucketType
        });

        return set.add('a1', 'a2', 'a3').remove('a2', 'a1').value()
        .then(function (v) {
            v.should.be.eql(['a3']);
        });
    });

    it('set keeps unique values', function () {
        var set = new Client.CRDT.Set(client, {
            bucket: bucket,
            type: bucketType
        });

        return set
        .add('a1', 'a2', 'a3', 'a2', 'a2', 'a3').value()
        .then(function (v) {
            v.should.be.eql(['a1', 'a2', 'a3']);
        });
    });

    it('should be able to save/load new set', function () {
        var set = new Client.CRDT.Set(client, {
            bucket: bucket,
            type: bucketType
        });

        expect(set.key()).to.eql(undefined);

        return set
        .add('a1', 'a2', 'a3', 'a2', 'a2', 'a3').save().call('value')
        .then(function (v) {
            set.key().should.be.a('string').and.have.length.gt(0);
            v.should.be.eql(['a1', 'a2', 'a3']);
        });
    });

    it('should be able to save/load new set with custom key', function () {
        var set = new Client.CRDT.Set(client, {
            bucket: bucket,
            type: bucketType,
            key: uniqueKey('set')
        });

        return set
        .add('a1', 'a2', 'a3', 'a2', 'a2', 'a3')
        .save()
        .call('value')
        .then(function (v) {
            v.should.be.eql(['a1', 'a2', 'a3']);
        });
    });

    it('should be able to save/load existing set', function () {
        var key = uniqueKey('set');
        var set = new Client.CRDT.Set(client, {
            bucket: bucket,
            type: bucketType,
            key: key
        });

        return set
        .add('a1', 'a2', 'a3', 'a2', 'a2', 'a3').save().call('value')
        .then(function (v) {
            var _set;
            v.should.be.eql(['a1', 'a2', 'a3']);

            _set = new Client.CRDT.Set(client, {
                bucket: bucket,
                type: bucketType,
                key: key
            });

            return _set.add('a4').remove('a2').save().call('value');
        })
        .then(function (v) {
            v.should.be.eql(['a1', 'a3', 'a4']);

            return set.add('a5').value();
        })
        .then(function (v) {
            v.should.be.eql(['a1', 'a3', 'a4', 'a5']);
        });
    });
});
