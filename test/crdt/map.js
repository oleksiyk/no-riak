'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

// var Promise = require('bluebird');
// var _ = require('lodash');
var Riak  = require('../..');
var client = new Riak.Client();

var bucket = 'no_riak_test_crdt_map_bucket';
var bucketType = 'no_riak_test_crdt_map';

describe('CRDT Map', function () {
    it('should create new Map', function () {
        var map = new Riak.CRDT.Map(client, {
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
        var map = new Riak.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        var v = map
        .update('key1', new Riak.CRDT.Counter().increment(5))
        .value();

        v.should.be.an('object');
        v.should.have.property('key1');
        v.key1.toNumber().should.be.eql(5);
    });

    it('add multiple fields', function () {
        var map = new Riak.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        var v = map
        .update('key1', new Riak.CRDT.Counter().increment(-5))
        .update('key2', new Riak.CRDT.Set().add('a1', 'a2', 'a3').remove('a2'))
        .value();
        v.should.be.an('object');
        v.should.have.property('key1');
        v.should.have.property('key2');
        v.key1.toNumber().should.be.eql(-5);
        v.key2.should.be.eql(['a1', 'a3']);
    });

    it('should save/load', function () {
        var map = new Riak.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        return map
        .update('key1', new Riak.CRDT.Counter().increment(-5))
        .update('key2', new Riak.CRDT.Set().add('a1', 'a2', 'a3').remove('a2'))
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

    it('save/load empty map', function () {
        var map = new Riak.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        return map.save().call('load').call('value').then(function () {
            return map.save();
        });
    });

    it('remove field before save', function () {
        var map = new Riak.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        return map
        .update('key1', new Riak.CRDT.Counter().increment(-5))
        .update('key2', new Riak.CRDT.Set().add('a1', 'a2', 'a3').remove('a2'))
        .remove('key1', Riak.CRDT.Counter)
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
        var map = new Riak.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        return map
        .update('key1', new Riak.CRDT.Counter().increment(-5))
        .update('key2', new Riak.CRDT.Set().add('a1', 'a2', 'a3').remove('a2'))
        .save()
        .then(function () {
            return map.remove('key1', Riak.CRDT.Counter).save();
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
        var map = new Riak.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        return map
        .update('key1', new Riak.CRDT.Counter().increment(-5))
        .update('key1', new Riak.CRDT.Set().add('a1', 'a2', 'a3'))
        .save()
        .call('value')
        .then(function (v) {
            v.should.be.an('object');
            v.should.have.property('key1');
            v.key1.should.be.eql(['a1', 'a2', 'a3']);
        });
    });

    it('update same field with different type on saved object', function () {
        var map = new Riak.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        return map
        .update('key1', new Riak.CRDT.Counter().increment(-5))
        .save()
        .then(function () {
            return map.update('key1', new Riak.CRDT.Set().add('a1', 'a2', 'a3')).save();
        })
        .call('value')
        .then(function (v) {
            v.should.be.an('object');
            v.should.have.property('key1');
            v.key1.should.be.eql(['a1', 'a2', 'a3']);
        });
    });

    it('get() returns CRDT instance', function () {
        var map = new Riak.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        var v;

        var counter = map
            .update('key1', new Riak.CRDT.Counter().increment(-5))
            .update('key2', new Riak.CRDT.Set().add('a1', 'a2', 'a3'))
            .get('key2');

        counter.should.be.an.instanceOf(Riak.CRDT.Set);

        counter.remove('a2');

        v = map.value();
        v.should.be.an('object');
        v.should.have.property('key1');
        v.should.have.property('key2');
        v.key2.should.be.eql(['a1', 'a3']);
    });

    it('get() returns undefined for not found field', function () {
        var map = new Riak.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        expect(map.get('key2')).to.eql(undefined);
    });

    it('get() update overwritten loaded field', function () {
        var map = new Riak.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });
        var _map;

        return map
        .update('key1', new Riak.CRDT.Counter().increment(-5))
        .save()
        .then(function () {
            _map = new Riak.CRDT.Map(client, {
                bucket: bucket,
                type: bucketType,
                key: map.key()
            });

            expect(_map.get('key1')).to.eql(undefined);
            return _map.load().call('value');
        })
        .then(function (v) {
            v.key1.toNumber().should.be.eql(-5);

            _map.update('key1', new Riak.CRDT.Counter().increment(1));
            _map.get('key1').should.be.an.instanceof(Riak.CRDT.Counter);
            _map.get('key1').increment(1);

            return _map.save();
        })
        .then(function () {
            return map.load().call('value');
        })
        .then(function (v) {
            v.key1.toNumber().should.be.eql(-3);
        });
    });

    it('should save/load new instance with provided key', function () {
        var map = new Riak.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType,
            key: uniqueKey('map')
        });

        return map
        .update('key1', new Riak.CRDT.Counter().increment(-5))
        .update('key2', new Riak.CRDT.Set().add('a1', 'a2', 'a3'))
        .save()
        .call('value')
        .then(function (v) {
            v.should.be.an('object');
            v.should.have.property('key1');
            v.should.have.property('key2');
            v.key1.toNumber().should.be.eql(-5);
            v.key2.should.be.eql(['a1', 'a2', 'a3']);
        });
    });

    it('should load and update existing map', function () {
        var map = new Riak.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });
        var _map;

        return map
        .update('key1', new Riak.CRDT.Counter().increment(-5))
        .update('key2', new Riak.CRDT.Set().add('a1', 'a2', 'a3'))
        .save()
        .then(function () {
            _map = new Riak.CRDT.Map(client, {
                bucket: bucket,
                type: bucketType,
                key: map.key()
            });

            expect(_map.get('key1')).to.eql(undefined);
            expect(_map.get('key2')).to.eql(undefined);

            return _map.load().call('value');
        })
        .then(function (v) {
            v.should.be.an('object');
            v.should.have.property('key1');
            v.should.have.property('key2');
            v.key1.toNumber().should.be.eql(-5);
            v.key2.should.be.eql(['a1', 'a2', 'a3']);

            _map.get('key1').should.be.an.instanceof(Riak.CRDT.Counter);
            _map.get('key2').should.be.an.instanceof(Riak.CRDT.Set);

            _map.get('key1').increment(1);
            _map.get('key2').remove('a2');

            return _map.save();
        })
        .then(function () {
            return map.load().call('value');
        })
        .then(function (v) {
            v.key1.toNumber().should.be.eql(-4);
            v.key2.should.be.eql(['a1', 'a3']);
        });
    });

    it('map in map', function () {
        var map = new Riak.CRDT.Map(client, {
            bucket: bucket,
            type: bucketType
        });

        return map
        .update('key1', new Riak.CRDT.Map().update('key1_1', new Riak.CRDT.Counter().increment(-5)))
        .save()
        .call('value')
        .then(function (v) {
            var counter;
            v.should.be.an('object');
            v.should.have.property('key1');
            v.key1.should.be.an('object').and.have.property('key1_1');
            v.key1.key1_1.toNumber().should.be.eql(-5);

            counter = map.get('key1').get('key1_1');
            counter.should.be.an.instanceOf(Riak.CRDT.Counter);

            counter.increment(2);

            return map.save().call('value');
        })
        .then(function (v) {
            v.should.be.an('object');
            v.should.have.property('key1');
            v.key1.should.be.an('object').and.have.property('key1_1');
            v.key1.key1_1.toNumber().should.be.eql(-3);
        });
    });
});
