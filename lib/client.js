'use strict';

var Promise    = require('bluebird');
var Pool       = require('./pool');
var _          = require('lodash');
var Connection = require('./connection');
var WRRPool    = require('wrr-pool');

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
        autoJSON: true,
        connectionTimeout: 3000,
        connectionBufSize: 256 * 1024,
        pool: {
            create: self._createConnection.bind(self),
            release: self._releaseConnection.bind(self)
        },
        auth: false,
        tls: {
            // secureProtocol: 'SSLv23_method',
            rejectUnauthorized: false,
            // `riak-admin security ciphers` shows a different list, but these are known to work:
            ciphers: 'DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA256:DHE-RSA-AES256-SHA:AES128-SHA256:AES128-SHA:AES256-SHA256:AES256-SHA:RC4-SHA'
        }
    });

    self.servers = new WRRPool(); // weighted round robin pool of servers

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
        initialBufferSize: this.options.connectionBufSize,
        auth: this.options.auth,
        tls: this.options.tls
    });
};

Client.prototype._releaseConnection = function (c) {
    c.close();
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

Client.prototype.init = function () {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbSetClientIdReq', {
            client_id: self.options.clientId
        });
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

    return self.pool.connection(function (connection) {
        return connection.send('RpbGetServerInfoReq');
    });
};

Client.prototype.ping = function () {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbPingReq');
    });
};

/**
 * Get value from bucket
 *
 * @param  {Object} params { bucket, key, r, pr, basic_quorum, notfound_ok ...}
 * @return {Promise}
 */
Client.prototype.get = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbGetReq', params).then(function (result) {
            return self._convertRpbGetResponse(result);
        });
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

        return self.pool.connection(function (connection) {
            return connection.send('RpbPutReq', params).then(function (result) {
                return self._convertRpbGetResponse(result);
            });
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

    return self.pool.connection(function (connection) {
        return connection.send('RpbDelReq', params);
    });
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

    return self.pool.connection(function (connection) {
        return connection.send('RpbListBucketsReq', params).then(function (results) {
            return _.flatMap(results, 'buckets');
        });
    });
};

/**
 * List keys in a bucket
 * @param  {Object} params { bucket, timeout, type }
 * @return {Promise}
 */
Client.prototype.listKeys = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbListKeysReq', params).then(function (results) {
            return _.flatMap(results, 'keys');
        });
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

    return self.pool.connection(function (connection) {
        return connection.send('RpbIndexReq', params).then(function (results) {
            return _.reduce(results, function (acc, cur) {
                acc.results = acc.results.concat(cur.keys || cur.results || []);
                if (cur.continuation) {
                    acc.continuation = cur.continuation;
                }
                return acc;
            }, { results: [] });
        });
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

        return self.pool.connection(function (connection) {
            return connection.send('RpbMapRedReq', params).then(function (results) {
                var result = [];

                results.forEach(function (r) {
                    result[r.phase] = Array.prototype.concat(result[r.phase] || [], JSON.parse(r.response));
                });

                return result;
            });
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

    return self.pool.connection(function (connection) {
        return connection.send('RpbGetReq', {
            bucket: params.bucket,
            key: params.key,
            type: params.type,
            head: true,
            deletedvclock: true
        })
        .then(function (result) {
            if (result) {
                params.vclock = result.vclock;
            }

            if (self.options.autoJSON && _.isPlainObject(params.content.value)) {
                params.content.value = JSON.stringify(params.content.value);
                params.content.content_type = 'application/json';
            }
            return connection.send('RpbPutReq', params);
        });
    });
};

Client.prototype.updateCounter = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbCounterUpdateReq', params).then(function (result) {
            if (result !== null) {
                return result.value;
            }
        });
    });
};

Client.prototype.getCounter = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbCounterGetReq', params).then(function (result) {
            if (result !== null) {
                return result.value;
            }
        });
    });
};

Client.prototype.getBucket = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbGetBucketReq', params).then(function (result) {
            ['pr', 'r', 'w', 'pw', 'dw', 'rw'].forEach(function (p) {
                result.props[p] = riakQuorumValues[result.props[p]] || result.props[p];
            });
            return result;
        });
    });
};

Client.prototype.setBucket = function (params) {
    var self = this;

    ['pr', 'r', 'w', 'pw', 'dw', 'rw'].forEach(function (p) {
        if (params && params.props && (typeof params.props[p] === 'string')) {
            params.props[p] = riakQuorumValues[params.props[p]] || params.props[p];
        }
    });

    return self.pool.connection(function (connection) {
        return connection.send('RpbSetBucketReq', params);
    });
};

Client.prototype.resetBucket = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbResetBucketReq', params);
    });
};

Client.prototype.getBucketType = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbGetBucketTypeReq', params).then(function (result) {
            ['pr', 'r', 'w', 'pw', 'dw', 'rw'].forEach(function (p) {
                result.props[p] = riakQuorumValues[result.props[p]] || result.props[p];
            });
            return result;
        });
    });
};

Client.prototype.setBucketType = function (params) {
    var self = this;

    ['pr', 'r', 'w', 'pw', 'dw', 'rw'].forEach(function (p) {
        if (params && params.props && (typeof params.props[p] === 'string')) {
            params.props[p] = riakQuorumValues[params.props[p]] || params.props[p];
        }
    });

    return self.pool.connection(function (connection) {
        return connection.send('RpbSetBucketTypeReq', params);
    });
};

Client.prototype.getSearchIndex = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbYokozunaIndexGetReq', params);
    });
};

Client.prototype.putSearchIndex = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbYokozunaIndexPutReq', params);
    });
};

Client.prototype.delSearchIndex = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbYokozunaIndexDeleteReq', params);
    });
};

Client.prototype.getSearchSchema = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbYokozunaSchemaGetReq', params);
    });
};

Client.prototype.putSearchSchema = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbYokozunaSchemaPutReq', params);
    });
};

Client.prototype.search = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('RpbSearchQueryReq', params).then(function (result) {
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
    });
};

Client.prototype.dtFetch = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('DtFetchReq', params);
    });
};

Client.prototype.dtUpdate = function (params) {
    var self = this;

    return self.pool.connection(function (connection) {
        return connection.send('DtUpdateReq', params);
    });
};
