'use strict';

/* global describe, it, before, after, expect, uniqueKey  */

var Promise = require('bluebird');
var Client  = require('..');
var _       = require('lodash');

var bucket = 'no-riak-test-kv';
var client = new Client();

describe('Key/Value operations', function () {
    before(function () {
        return client.setBucket({
            bucket: bucket,
            props: {
                allow_mult: true
            }
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
        var _client = new Client({
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
            result.content.should.have.length(1);
            result.content[0].should.have.property('value');
            result.content[0].value.toString().should.be.eql('hello');
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

    it('listBuckets', function () {
        this.timeout(5000);
        return client.listBuckets()
        .then(function (result) {
            result.should.be.an('array');
            result.should.have.length.gt(0);
            result[0].should.be.a('string');
            result.should.include(bucket);
        });
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
        .spread(function (_keys, continuation) {
            _keys.should.be.an('array').and.have.length(2);
            continuation.should.be.a('string');

            return client.index({
                bucket: bucket,
                index: 'no-riak-test_bin',
                qtype: 0,
                max_results: 2,
                continuation: continuation,
                key: indexValue
            });
        })
        .spread(function (_keys, continuation) {
            _keys.should.be.an('array').and.have.length(1);
            expect(continuation).to.eql(null);
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
        .spread(function (_keys, continuation) {
            _keys.should.be.an('array').and.have.length(3);
            expect(continuation).to.eql(null);
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
        .spread(function (results, continuation) {
            results.should.be.an('array').and.have.length(3);
            results[0].should.be.an('object');
            results[0].should.have.property('key');
            results[0].should.have.property('value');
            expect(continuation).to.eql(null);
        });
    });

    it('2i invalid query', function () {
        return client.index({
            bucket: bucket,
            index: 'no-riak-test_bin',
            qtype: 0
        }).should.be.rejectedWith('Invalid equality query');
    });

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
