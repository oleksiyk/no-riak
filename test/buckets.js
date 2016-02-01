'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

// var Promise = require('bluebird');
// var _ = require('lodash');
var Client  = require('..');

var client = new Client();
var bucket = 'no-riak-test-bucket';
var bucketType = 'no_riak_test_bucket_type';

describe('Buckets API', function () {
    it('listBuckets', function () {
        this.timeout(5000);
        return client.put({
            bucket: bucket,
            content: {
                value: 'hello'
            }
        })
        .then(function () {
            return client.listBuckets();
        })
        .then(function (result) {
            result.should.be.an('array');
            result.should.have.length.gt(0);
            result[0].should.be.a('string');
            result.should.include(bucket);
        });
    });

    it('getBucket', function () {
        return client.getBucket({
            bucket: bucket
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('props');
            result.props.should.be.an('object');
            result.props.should.have.property('n_val');
            result.props.should.have.property('allow_mult');
            result.props.should.have.property('last_write_wins');
            result.props.should.have.property('basic_quorum');
            result.props.should.have.property('notfound_ok');
        });
    });

    it('setBucket', function () {
        return client.setBucket({
            bucket: bucket,
            props: {
                allow_mult: true,
                r: 1,
                notfound_ok: false,
                basic_quorum: true
            }
        })
        .then(function (result) {
            expect(result).to.eql(null);
        })
        .then(function () {
            return client.getBucket({
                bucket: bucket
            });
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('props');
            result.props.should.be.an('object');
            result.props.should.have.property('r', 1);
            result.props.should.have.property('allow_mult', true);
            result.props.should.have.property('basic_quorum', true);
            result.props.should.have.property('notfound_ok', false);
        });
    });

    it('setBucket - string quorum values (r, pr, w, ..)', function () {
        return client.setBucket({
            bucket: bucket,
            props: {
                r: 'all',
                w: 'one'
            }
        })
        .then(function (result) {
            expect(result).to.eql(null);
        })
        .then(function () {
            return client.getBucket({
                bucket: bucket
            });
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('props');
            result.props.should.be.an('object');
            result.props.should.have.property('r', 'all');
            result.props.should.have.property('w', 'one');
        });
    });

    it('setBucket - numeric quorum values (r, pr, w, ..)', function () {
        return client.setBucket({
            bucket: bucket,
            props: {
                r: 4294967292,
                w: 4294967294
            }
        })
        .then(function (result) {
            expect(result).to.eql(null);
        })
        .then(function () {
            return client.getBucket({
                bucket: bucket
            });
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('props');
            result.props.should.be.an('object');
            result.props.should.have.property('r', 'all');
            result.props.should.have.property('w', 'one');
        });
    });

    it('resetBucket', function () {
        return client.resetBucket({
            bucket: bucket
        })
        .then(function (result) {
            expect(result).to.eql(null);
        })
        .then(function () {
            return client.getBucket({
                bucket: bucket
            });
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('props');
            result.props.should.be.an('object');
            result.props.should.have.property('r', 'quorum');
            result.props.should.have.property('allow_mult', false);
            result.props.should.have.property('basic_quorum', false);
            result.props.should.have.property('notfound_ok', true);
        });
    });

    it('getBucketType - missing bucket type', function () {
        return client.getBucketType({
            type: uniqueKey('bucket-type')
        }).should.be.rejectedWith('Invalid bucket type');
    });

    it('getBucketType', function () {
        return client.getBucketType({
            type: bucketType
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('props');
            result.props.should.be.an('object');
            result.props.should.have.property('allow_mult', true);
        });
    });

    it('setBucketType - not active', function () {
        return client.setBucketType({
            type: uniqueKey('bucket-type'),
            props: {
                allow_mult: true
            }
        }).should.be.rejectedWith('Invalid bucket properties: not_active');
    });

    it('setBucketType', function () {
        return client.setBucketType({
            type: bucketType,
            props: {
                notfound_ok: false,
                basic_quorum: true
            }
        })
        .then(function () {
            return client.getBucketType({
                type: bucketType
            });
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('props');
            result.props.should.be.an('object');
            result.props.should.have.property('allow_mult', true);
            result.props.should.have.property('notfound_ok', false);
            result.props.should.have.property('basic_quorum', true);
        });
    });

    it('setBucketType - string quorum values (r, pr, w, ..)', function () {
        return client.setBucketType({
            type: bucketType,
            props: {
                r: 'all',
                w: 'one'
            }
        })
        .then(function (result) {
            expect(result).to.eql(null);
        })
        .then(function () {
            return client.getBucketType({
                type: bucketType
            });
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('props');
            result.props.should.be.an('object');
            result.props.should.have.property('r', 'all');
            result.props.should.have.property('w', 'one');
        });
    });
});
