'use strict';

var Promise    = require('bluebird');
var Pool       = require('./pool');
var _          = require('lodash');
var Connection = require('./connection');
var WRRPool    = require('wrr-pool');
var Protocol   = require('./protocol');

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

var multipleResponse = {
    RpbListKeysReq: true,
    RpbListBucketsReq: true,
    RpbMapRedReq: true,
    RpbIndexReq: true
};

function Client(options) {
    var self = this;

    self.options = _.defaultsDeep(options || {}, {
        clientId: 'no-riak-client',
        connectionString: '127.0.0.1:8087', // 'host:port:weight,host:port:weight,..', e.g.: '10.0.1.1:8087:10,10.0.1.2:8087:5,10.0.1.3:8087:2'
        autoJSON: true,
        auth: false,
        connectionTimeout: 3000,
        connectionBufSize: 256 * 1024,
        pool: {
            create: self._createConnection.bind(self),
            release: self._releaseConnection.bind(self)
        }
    });

    self.servers = new WRRPool(); // weighted round robin pool of servers

    self.protocol = new Protocol({
        bufferSize: 256 * 1024,
        resultCopy: true
    });

    self.options.connectionString.split(',').map(function (hostStr) {
        var h = hostStr.trim().split(':');

        self.servers.add({
            host   : h[0],
            port   : parseInt(h[1])
        }, parseInt(h[2] || 10));
    });

    self.pool = new Pool(self.options.pool);
}

module.exports = Client;

Client.prototype._createConnection = function () {
    var server = this.servers.next();

    return new Connection({
        host: server.host,
        port: server.port,
        connectionTimeout: this.options.connectionTimeout,
        initialBufferSize: this.options.connectionBufSize
    });
};

Client.prototype._releaseConnection = function (c) {
    c.close();
};

/**
 * Process raw buffer received from Riak
 */
Client.prototype._processTask = function (task, data) {
    var self = this, result, done;

    try {
        result = self.protocol.read(data).Response().result;
    } catch (err) {
        return task.reject(err);
    }

    if (!task.multiple) {
        task.resolve(result);
    } else {
        done = result.done;
        delete result.done;

        if (!_.isEmpty(result)) {
            task.result.push(result);
        }

        if (done) {
            task.resolve(task.result);
        }
    }
};

/**
 * Send request to Riak
 * @param  {String} request Riak PB message name
 * @param  {Obkect} params  message params
 * @return {Promise}
 */
Client.prototype._send = function (request, params) {
    var self = this, buffer;

    return self.pool.connection(function (connection) {
        return new Promise(function (resolve, reject) {
            var task = {
                result: [],
                multiple: multipleResponse[request] || false,
                resolve: resolve,
                reject: reject
            };

            task.process = _.bind(self._processTask, self, task);

            buffer = self.protocol.write().Request(request, params).result;

            connection.send(buffer, task).catch(function (err) {
                reject(err);
            });
        });
    });
};

/**
 * Send several requests in sequence on the same connection
 * @param  {Array} requests Array of requests (Riak PB message names)
 * @param  {Function} reducer Function that will be called before each iteration and should return params for next request
 * @return {Promise} Final result of last request in sequence
 */
Client.prototype._reduce = function (requests, reducer) {
    var self = this, buffer;

    return self.pool.connection(function (connection) {
        return Promise.reduce(requests, function (acc, request, index, len) {
            return Promise.try(_.partial(reducer, acc, request, index, len)).then(function (params) {
                return new Promise(function (resolve, reject) {
                    var task = {
                        result: [],
                        multiple: multipleResponse[request] || false,
                        resolve: resolve,
                        reject: reject
                    };

                    task.process = _.bind(self._processTask, self, task);

                    buffer = self.protocol.write().Request(request, params).result;

                    connection.send(buffer, task).catch(function (err) {
                        reject(err);
                    });
                });
            });
        }, null);
    });
};


Client.prototype.init = function () {
    var self = this;

    return self._send('RpbSetClientIdReq', {
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

    return self._send('RpbGetServerInfoReq');
};

Client.prototype.ping = function () {
    var self = this;

    return self._send('RpbPingReq');
};

Client.prototype._convertRpbGetResponse = function (result) {
    var self = this;

    if (result) {
        if (result.vclock) {
            result.vclock = result.vclock.toString('base64');
        }

        if (result.content && self.options.autoJSON) {
            result.content = _.map(result.content, function (c) {
                if (c.value && c.content_type === 'application/json') {
                    c.value = JSON.parse(c.value.toString('utf8'));
                }
                return c;
            });
        }
    }

    return result;
};

/**
 * Get value from bucket
 *
 * @param  {Object} params { bucket, key, r, pr, basic_quorum, notfound_ok ...}
 * @return {Promise}
 */
Client.prototype.get = function (params) {
    var self = this;

    return self._send('RpbGetReq', params).then(function (result) {
        return self._convertRpbGetResponse(result);
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

        if (typeof params.vclock === 'string') {
            params.vclock = new Buffer(params.vclock, 'base64');
        }

        return self._send('RpbPutReq', params).then(function (result) {
            return self._convertRpbGetResponse(result);
        });
    });
};

/**
 * Delete key from bucket
 * @param  {Object} params { bucket, key, rw, vlock, ..}
 * @return {Promise}
 */
Client.prototype.del = function (params) {
    var self = this;

    if (typeof params.vclock === 'string') {
        params.vclock = new Buffer(params.vclock, 'base64');
    }

    return self._send('RpbDelReq', params);
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
    return self._send('RpbListBucketsReq', params).then(function (results) {
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

    return self._send('RpbListKeysReq', params).then(function (results) {
        return _.flatMap(results, 'keys');
    });
};

/**
 * Run 2i index search
 *
 * @param  {Object} params { bucket, index, qtype, key, ... }
 * @return {Promise}        Promise { results, continuation }
 */
Client.prototype.index = function (params) {
    var self = this;

    params = params || {};
    params.stream = true;
    return self._send('RpbIndexReq', params).then(function (results) {
        return _.reduce(results, function (acc, cur) {
            acc.results = acc.results.concat(cur.keys || cur.results || []);
            if (cur.continuation) {
                acc.continuation = cur.continuation;
            }
            return acc;
        }, { results: [] });
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

        return self._send('RpbMapRedReq', params).then(function (results) {
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

    return self._reduce(['RpbGetReq', 'RpbPutReq'], function (prev, request, index) {
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
    return self._send('RpbCounterUpdateReq', params).then(function (result) {
        if (result !== null) {
            return result.value;
        }
    });
};

Client.prototype.getCounter = function (params) {
    var self = this;
    return self._send('RpbCounterGetReq', params).then(function (result) {
        if (result !== null) {
            return result.value;
        }
    });
};

Client.prototype.getBucket = function (params) {
    var self = this;
    return self._send('RpbGetBucketReq', params).then(function (result) {
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
    return self._send('RpbSetBucketReq', params);
};

Client.prototype.resetBucket = function (params) {
    var self = this;
    return self._send('RpbResetBucketReq', params);
};

Client.prototype.getBucketType = function (params) {
    var self = this;

    return self._send('RpbGetBucketTypeReq', params).then(function (result) {
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
    return self._send('RpbSetBucketTypeReq', params);
};

Client.prototype.getSearchIndex = function (params) {
    var self = this;
    return self._send('RpbYokozunaIndexGetReq', params);
};

Client.prototype.putSearchIndex = function (params) {
    var self = this;
    return self._send('RpbYokozunaIndexPutReq', params);
};

Client.prototype.delSearchIndex = function (params) {
    var self = this;
    return self._send('RpbYokozunaIndexDeleteReq', params);
};

Client.prototype.getSearchSchema = function (params) {
    var self = this;
    return self._send('RpbYokozunaSchemaGetReq', params);
};

Client.prototype.putSearchSchema = function (params) {
    var self = this;
    return self._send('RpbYokozunaSchemaPutReq', params);
};

Client.prototype.search = function (params) {
    var self = this;
    return self._send('RpbSearchQueryReq', params).then(function (result) {
        if (result) {
            result.docs = (result.docs || []).map(function (doc) {
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
