'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

// var Promise = require('bluebird');
var Riak  = require('..');
// var _       = require('lodash');
var bucket = 'no-riak-test-kv';
var client = new Riak.Client();

describe('Key/Value operations', function () {
    before(function () {
        return client.setBucket({
            bucket: bucket,
            props: {
                allow_mult: true
            }
        });
    });

    it('put', function () {
        return client.put({
            bucket: bucket,
            content: {
                value: '123'
            }
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('key').that.is.a('string');
            result.should.not.have.property('content');
            result.should.not.have.property('vclock');
        });
    });

    it('put - return_head: true', function () {
        return client.put({
            bucket: bucket,
            content: {
                value: '123'
            },
            return_head: true
        })
        .then(function (result) {
            result.should.be.an('object');
            result.should.have.property('content').that.is.an('array');
            result.should.have.property('vclock').that.is.a('string');
            result.content.should.have.length(1);
            result.content[0].should.have.property('value');
            expect(result.content[0].value).to.eql(null);
            result.content[0].should.have.property('vtag').that.is.a('string');
            result.content[0].should.have.property('last_mod').that.is.a('number');
            result.content[0].should.have.property('last_mod_usecs').that.is.a('number');
        });
    });

    it('put - return_body: true', function () {
        return client.put({
            bucket: bucket,
            content: {
                value: '123'
            },
            return_body: true
        })
        .then(function (result) {
            result.should.be.an('object');
            result.should.have.property('content').that.is.an('array');
            result.should.have.property('vclock').that.is.a('string');
            result.content.should.have.length(1);
            result.content[0].should.have.property('value');
            result.content[0].value.toString('utf8').should.be.eql('123');
            result.content[0].should.have.property('vtag').that.is.a('string');
            result.content[0].should.have.property('last_mod').that.is.a('number');
            result.content[0].should.have.property('last_mod_usecs').that.is.a('number');
        });
    });

    it('put/get UTF8 string', function () {
        var str = '人人生而自由，在尊嚴和權利上一律平等。';
        return client.put({
            bucket: bucket,
            content: {
                value: str
            }
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('key').that.is.a('string');

            return client.get({
                bucket: bucket,
                key: result.key
            });
        })
        .then(function (result) {
            result.should.be.an('object');
            result.should.have.property('content').that.is.an('array');
            result.should.have.property('vclock').that.is.a('string');
            result.content.should.have.length(1);
            result.content[0].should.have.property('value');
            result.content[0].value.toString('utf8').should.be.eql(str);
            result.content[0].should.have.property('vtag').that.is.a('string');
            result.content[0].should.have.property('last_mod').that.is.a('number');
            result.content[0].should.have.property('last_mod_usecs').that.is.a('number');
        });
    });

    it('get/put with vclock', function () {
        return client.put({
            bucket: bucket,
            content: {
                value: '123'
            },
            return_head: true
        })
        .then(function (result) {
            result.should.have.property('vclock').that.is.a('string');
            return client.put({
                bucket: bucket,
                key: result.key,
                vclock: result.vclock,
                content: {
                    value: '456'
                }
            });
        });
    });

    it('put - number - rejected', function () {
        return client.put({
            bucket: bucket,
            content: {
                value: 123
            }
        }).should.be.rejected;
    });

    it('put/get - plain object (autoJSON = true)', function () {
        var obj = {
            hello: 'world!'
        };
        return client.put({
            bucket: bucket,
            content: {
                value: obj
            }
        })
        .then(function (result) {
            result.should.be.an('object').and.have.property('key').that.is.a('string');

            return client.get({
                bucket: bucket,
                key: result.key
            });
        })
        .then(function (result) {
            result.should.be.an('object');
            result.should.have.property('content').that.is.an('array');
            result.should.have.property('vclock').that.is.a('string');
            result.content.should.have.length(1);
            result.content[0].should.have.property('value');
            result.content[0].value.should.be.eql(obj);
            result.content[0].should.have.property('vtag').that.is.a('string');
            result.content[0].should.have.property('last_mod').that.is.a('number');
            result.content[0].should.have.property('last_mod_usecs').that.is.a('number');
        });
    });

    it('put - plain object (autoJSON = false)', function () {
        var _client = new Riak.Client({
            autoJSON: false
        });

        return _client.put({
            bucket: bucket,
            content: {
                value: {
                    hello: 'world!'
                }
            }
        }).should.be.rejected;
    });

    it('put/get with specified key', function () {
        var key = uniqueKey('key');

        return client.put({
            bucket: bucket,
            key: key,
            content: {
                value: 'hello'
            }
        })
        .then(function (result) {
            expect(result).to.eql(null);

            return client.get({
                bucket: bucket,
                key: key
            });
        })
        .then(function (result) {
            result.should.be.an('object');
            result.should.have.property('content').that.is.an('array');
            result.should.have.property('vclock').that.is.an('string');
            result.content.should.have.length(1);
            result.content[0].should.have.property('value');
            result.content[0].value.toString().should.be.eql('hello');
        });
    });

    it('get with head=true', function () {
        var key = uniqueKey('key');

        return client.put({
            bucket: bucket,
            key: key,
            content: {
                value: 'hello'
            }
        })
        .then(function (result) {
            expect(result).to.eql(null);

            return client.get({
                bucket: bucket,
                key: key,
                head: true
            });
        })
        .then(function (result) {
            result.should.be.an('object');
            result.should.have.property('content').that.is.an('array');
            result.should.have.property('vclock').that.is.an('string');
            result.content.should.have.length(1);
            result.content[0].should.have.property('value');
            expect(result.content[0].value).to.eql(null);
        });
    });

    it('get wrong key', function () {
        var key = uniqueKey('key');

        return client.get({
            bucket: bucket,
            key: key
        })
        .then(function (result) {
            expect(result).to.eql(null);
        });
    });

    it('put twice without vclock - siblings (allow_mult=true)', function () {
        var key = uniqueKey('key');

        return client.put({
            bucket: bucket,
            key: key,
            content: {
                value: 'hello1'
            }
        })
        .then(function () {
            return client.put({
                bucket: bucket,
                key: key,
                content: {
                    value: 'hello2'
                }
            });
        })
        .then(function () {
            return client.get({
                bucket: bucket,
                key: key
            });
        })
        .then(function (result) {
            result.should.be.an('object');
            result.should.have.property('content').that.is.an('array');
            result.content.should.have.length(2);
            result.content[0].should.have.property('value');
            result.content[0].value.toString().should.be.eql('hello1');
            result.content[1].should.have.property('value');
            result.content[1].value.toString().should.be.eql('hello2');
        });
    });

    it('update twice - no siblings (allow_mult=true)', function () {
        var key = uniqueKey('key');

        return client.update({
            bucket: bucket,
            key: key,
            content: {
                value: 'hello1'
            }
        })
        .then(function () {
            return client.update({
                bucket: bucket,
                key: key,
                content: {
                    value: 'hello2'
                }
            });
        })
        .then(function () {
            return client.get({
                bucket: bucket,
                key: key
            });
        })
        .then(function (result) {
            result.should.be.an('object');
            result.should.have.property('content').that.is.an('array');
            result.content.should.have.length(1);
            result.content[0].should.have.property('value');
            result.content[0].value.toString().should.be.eql('hello2');
        });
    });

    it('del - wrong key', function () {
        return client.del({
            bucket: bucket,
            key: 'no-such-key'
        }).should.eventually.eql(null);
    });

    it('del - wrong bucket', function () {
        return client.del({
            bucket: 'no-such-bucket',
            key: 'no-such-key'
        }).should.eventually.eql(null);
    });

    it('del', function () {
        var key = uniqueKey('key');

        return client.put({
            bucket: bucket,
            key: key,
            content: {
                value: 'hello'
            }
        })
        .then(function () {
            return client.del({
                bucket: bucket,
                key: key
            });
        })
        .delay(20) // easy way to avoid tobstones in a test (without using vclock)
        .then(function () {
            return client.get({
                bucket: bucket,
                key: key
            });
        }).should.eventually.be.eql(null);
    });

    it('del with vclock', function () {
        var key = uniqueKey('key');

        return client.put({
            bucket: bucket,
            key: key,
            content: {
                value: 'hello'
            },
            return_head: true
        })
        .then(function (result) {
            result.should.have.property('vclock').that.is.a('string');

            return client.del({
                bucket: bucket,
                key: key,
                vclock: result.vclock
            });
        })
        .then(function () {
            return client.get({
                bucket: bucket,
                key: key
            });
        }).should.eventually.be.eql(null);
    });

    it('listKeys', function () {
        var key = uniqueKey('key');

        return client.put({
            bucket: bucket,
            key: key,
            content: {
                value: 'hello'
            }
        })
        .then(function () {
            return client.listKeys({
                bucket: bucket
            });
        })
        .then(function (result) {
            result.should.be.an('array');
            result.should.have.length.gt(0);
            result[0].should.be.a('string');
            result.should.include(key);
        });
    });

    it('listKeys - empty bucket', function () {
        return client.listKeys({
            bucket: uniqueKey('nucket')
        })
        .then(function (result) {
            result.should.be.an('array').and.have.length(0);
        });
    });

    it('set counter', function () {
        var key = uniqueKey('counter');
        return client.updateCounter({
            bucket: bucket,
            key: key,
            amount: 5,
            returnvalue: true
        })
        .then(function (value) {
            value.toNumber().should.be.eql(5);
        });
    });

    it('get counter', function () {
        var key = uniqueKey('counter');
        return client.updateCounter({
            bucket: bucket,
            key: key,
            amount: 5
        })
        .then(function () {
            return client.getCounter({
                bucket: bucket,
                key: key
            });
        })
        .then(function (value) {
            value.toNumber().should.be.eql(5);
        });
    });
});
