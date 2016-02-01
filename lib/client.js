'use strict';

var Promise  = require('bluebird');
var Pool     = require('./pool');
var _        = require('lodash');

var riakQuorumValues = {
    4294967294 : 'one',
    one        : 4294967294,
    4294967293 : 'quorum',
    quorum     : 4294967293,
    4294967292 : 'all',
    all        : 4294967292,
    4294967291 : 'default',
    default    : 4294967291
};

function Client(options) {
    var self = this;

    self.options = _.defaultsDeep(options || {}, {
        clientId: 'no-riak-client',
        connectionString: '127.0.0.1:8087', // 'host:port:weight,host:port:weight,..', e.g.: '10.0.1.1:8087:10,10.0.1.2:8087:5,10.0.1.3:8087:2'
        autoJSON: true
    });

    self.pool = new Pool(self.options.connectionString, self.options.pool);
}

module.exports = Client;

Client.prototype.init = function () {
    var self = this;

    return self.pool.send('RpbSetClientIdReq', {
        client_id: self.options.clientId
    });
};

Client.prototype.end = function () {
    return self.pool.end();
};

/**
 * Get server info
 *
 * @return {Promise}
 */
Client.prototype.getServerInfo = function () {
    var self = this;

    return self.pool.send('RpbGetServerInfoReq');
};

/**
 * Get value from bucket
 *
 * @param  {Object} params { bucket, key, r, pr, basic_quorum, notfound_ok ...}
 * @return {Promise}
 */
Client.prototype.get = function (params) {
    var self = this;

    return self.pool.send('RpbGetReq', params).then(function (result) {
        if (result) {
            result.vclock = result.vclock.toString('base64');

            if (self.options.autoJSON) {
                result.content = _.map(result.content, function (c) {
                    if (c.content_type === 'application/json') {
                        c.value = JSON.parse(c.value.toString('utf8'));
                    }
                    return c;
                });
            }
        }

        return result;
    });
};

/**
 * But value in a bucket
 * @param  {Object} params { bucket, key, content, vclock, ...}
 * @return {Promise}
 */
Client.prototype.put = function (params) {
    var self = this;

    return Promise.try(function () {
        if (self.options.autoJSON && _.isPlainObject(params.content.value)) {
            params.content.value = JSON.stringify(params.content.value);
            params.content.content_type = 'application/json';
        }

        return self.pool.send('RpbPutReq', params);
    });
};

/**
 * Delete key from bucket
 * @param  {Object} params { bucket, key, rw, vlock, ..}
 * @return {Promise}
 */
Client.prototype.del = function (params) {
    var self = this;
    return self.pool.send('RpbDelReq', params);
};

/**
 * List buckets
 * @param  {Object} params { timeout, type }
 * @return {Promise}
 */
Client.prototype.listBuckets = function (params) {
    var self = this;

    params = params || {};
    params.stream = true;
    return self.pool.send('RpbListBucketsReq', params).then(function (results) {
        return _.flatMap(results, 'buckets');
    });
};

/**
 * List keys in a bucket
 * @param  {Object} params { bucket, timeout, type }
 * @return {Promise}
 */
Client.prototype.listKeys = function (params) {
    var self = this;

    return self.pool.send('RpbListKeysReq', params).then(function (results) {
        return _.flatMap(results, 'keys');
    });
};

/**
 * Run 2i index search
 *
 * @param  {Object} params { bucket, index, qtype, key, ... }
 * @return {Promise}        Promise for array of two results: [keys, continuation]. Use .spread(function(keys, cont))..
 */
Client.prototype.index = function (params) {
    var self = this;

    params = params || {};
    params.stream = true;
    return self.pool.send('RpbIndexReq', params).then(function (results) {
        return _.reduce(results, function (acc, cur) {
            acc[0] = acc[0].concat(cur.keys || cur.results || []);
            acc[1] = cur.continuation || null;
            return acc;
        }, [[], null]);
    });
};

/**
 * Run a map/reduce job, see test/kv.js for an example
 * @param  {Object} params
 * @return {Promise}
 */
Client.prototype.mapReduce = function (params) {
    var self = this;

    return Promise.try(function () {
        if (self.options.autoJSON && _.isPlainObject(params.request)) {
            params.request = JSON.stringify(params.request);
            params.content_type = 'application/json';
        }

        return self.pool.send('RpbMapRedReq', params).then(function (results) {
            var result = [];

            results.forEach(function (r) {
                result[r.phase] = Array.prototype.concat(result[r.phase] || [], JSON.parse(r.response));
            });

            return result;
        });
    });
};

/**
 * Experimental. Fetches head of the object to get its vclock before performing put request.
 * Both requests are sent on the same connection.
 *
 * @experimental
 *
 * @param  {Object} params
 * @return {Promise}        [description]
 */
Client.prototype.update = function (params) {
    var self = this;

    return self.pool.reduce(['RpbGetReq', 'RpbPutReq'], function (prev, request, index) {
        if (index === 0) {
            return {
                bucket: params.bucket,
                key: params.key,
                type: params.type,
                head: true,
                deletedvclock: true
            };
        }

        if (prev) {
            params.vclock = prev.vclock;
        }

        if (self.options.autoJSON && _.isPlainObject(params.content.value)) {
            params.content.value = JSON.stringify(params.content.value);
            params.content.content_type = 'application/json';
        }

        return params;
    });
};

Client.prototype.updateCounter = function (params) {
    var self = this;
    return self.pool.send('RpbCounterUpdateReq', params).then(function (result) {
        if (result !== null) {
            return result.value;
        }
    });
};

Client.prototype.getCounter = function (params) {
    var self = this;
    return self.pool.send('RpbCounterGetReq', params).then(function (result) {
        if (result !== null) {
            return result.value;
        }
    });
};

Client.prototype.getBucket = function (params) {
    var self = this;
    return self.pool.send('RpbGetBucketReq', params).then(function (result) {
        ['pr', 'r', 'w', 'pw', 'dw', 'rw'].forEach(function (p) {
            result.props[p] = riakQuorumValues[result.props[p]] || result.props[p];
        });
        return result;
    });
};

Client.prototype.setBucket = function (params) {
    var self = this;

    ['pr', 'r', 'w', 'pw', 'dw', 'rw'].forEach(function (p) {
        if (params && params.props && (typeof params.props[p] === 'string')) {
            params.props[p] = riakQuorumValues[params.props[p]] || params.props[p];
        }
    });
    return self.pool.send('RpbSetBucketReq', params);
};

Client.prototype.resetBucket = function (params) {
    var self = this;
    return self.pool.send('RpbResetBucketReq', params);
};

Client.prototype.getBucketType = function (params) {
    var self = this;

    return self.pool.send('RpbGetBucketTypeReq', params).then(function (result) {
        ['pr', 'r', 'w', 'pw', 'dw', 'rw'].forEach(function (p) {
            result.props[p] = riakQuorumValues[result.props[p]] || result.props[p];
        });
        return result;
    });
};

Client.prototype.setBucketType = function (params) {
    var self = this;

    ['pr', 'r', 'w', 'pw', 'dw', 'rw'].forEach(function (p) {
        if (params && params.props && (typeof params.props[p] === 'string')) {
            params.props[p] = riakQuorumValues[params.props[p]] || params.props[p];
        }
    });
    return self.pool.send('RpbSetBucketTypeReq', params);
};

Client.prototype.getSearchIndex = function (params) {
    var self = this;
    return self.pool.send('RpbYokozunaIndexGetReq', params);
};

Client.prototype.putSearchIndex = function (params) {
    var self = this;
    return self.pool.send('RpbYokozunaIndexPutReq', params);
};

Client.prototype.delSearchIndex = function (params) {
    var self = this;
    return self.pool.send('RpbYokozunaIndexDeleteReq', params);
};

Client.prototype.getSearchSchema = function (params) {
    var self = this;
    return self.pool.send('RpbYokozunaSchemaGetReq', params);
};

Client.prototype.putSearchSchema = function (params) {
    var self = this;
    return self.pool.send('RpbYokozunaSchemaPutReq', params);
};

Client.prototype.search = function (params) {
    var self = this;
    return self.pool.send('RpbSearchQueryReq', params).then(function (result) {
        if (result && result.docs) {
            result.docs = result.docs.map(function (doc) {
                var d = {};
                doc.fields.forEach(function (field) {
                    d[field.key] = field.value;
                });
                return d;
            });
        }

        return result;
    });
};
