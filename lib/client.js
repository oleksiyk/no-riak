'use strict';

var Promise    = require('bluebird');
var Pool       = require('./pool');
var _          = require('lodash');
var Connection = require('./connection');
var WRRPool    = require('wrr-pool');
var errors     = require('./errors');
var events     = require('events');
var util       = require('util');

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
        maxConnectionErrors: 3,
        maxConnectionErrorsPeriod: 60 * 1000,
        maxConnectionLifetime: 15 * 60 * 1000,
        retries: 3,
        pool: {},
        auth: false,
        tls: {
            // secureProtocol: 'SSLv23_method',
            rejectUnauthorized: false,
            // `riak-admin security ciphers` shows a different list, but these are known to work:
            ciphers: 'DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA256:DHE-RSA-AES256-SHA:AES128-SHA256:AES128-SHA:AES256-SHA256:AES256-SHA:RC4-SHA'
        }
    });

    self.servers = new WRRPool(); // weighted round robin pool of servers

    self.options.connectionString.trim().split(',').forEach(function (hostStr) {
        var h = hostStr.trim().split(':');

        if (h.length < 2) { return null; }

        self.servers.add({
            host: h[0],
            port: parseInt(h[1]),
            origWeight: parseInt(h[2] || 10),
            errors: []
        }, parseInt(h[2] || 10));

        return null;
    });

    if (self.servers.size() === 0) {
        throw new Error('No Riak servers were defined');
    }

    self.options.pool.create = self._createConnection.bind(self);
    self.options.pool.release = self._releaseConnection.bind(self);
    self.options.pool.check = self._checkConnection.bind(self);

    self.pool = new Pool(self.options.pool);

    self._pinging = {};
}

util.inherits(Client, events.EventEmitter);

module.exports = Client;

Client.prototype._createConnection = function () {
    var server = this.servers.next();

    if (server === null) {
        return null;
    }

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

Client.prototype._checkConnection = function (err, c) {
    var self = this, s, now = Date.now();

    if (err instanceof errors.RiakConnectionError) {
        s = self.servers.get({ host: c.host(), port: c.port() });

        s.value.errors.push(now);

        // drop error timestamps from older periods
        s.value.errors = _.dropWhile(s.value.errors, function (t) {
            return t < (now - self.options.maxConnectionErrorsPeriod);
        });

        if (s.value.errors.length >= self.options.maxConnectionErrors) {
            self.servers.updateWeight({ host: c.host(), port: c.port() }, 0);
            self._ping(c.host(), c.port());

            self.pool.close('free', function (_c) {
                return _c.host() === c.host() && _c.port() === c.port();
            });

            self.emit('net:hostdown', { host: c.host(), port: c.port() });
        }

        c.close();
        return self._createConnection();
    }

    if (Date.now() > (c.created + self.options.maxConnectionLifetime * (1 + Math.random() / 2))) { // replace expired connection
        c.close();
        return self._createConnection();
    }

    return c;
};


Client.prototype._ping = function (host, port) {
    var self = this, connection;

    if (self._pinging[host + ':' + port]) {
        return undefined;
    }

    self._pinging[host + ':' + port] = true;

    connection = new Connection({
        host: host,
        port: port,
        connectionTimeout: self.options.connectionTimeout,
        initialBufferSize: 1024,
        auth: self.options.auth,
        tls: self.options.tls
    });

    // keep sending ping request until the server is back, then restore it in the pool
    return (function _try() {
        var rand = 'no-riak-ping-' + Date.now();
        return connection.send('RpbGetReq', { bucket: rand, key: rand, r: 1, pr: 0, notfound_ok: true }).catch(function () {
            return Promise.delay(1000).then(_try);
        });
    }())
    .then(function () { // host is back
        var p = self.servers.get({ host: host, port: port });

        p.value.errors = [];
        self._pinging[host + ':' + port] = false;
        connection.close();
        self.servers.updateWeight({ host: host, port: port }, p.value.origWeight);

        self.emit('net:hostup', { host: host, port: port });
    });
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

Client.prototype._request = function (name, params) {
    var self = this;

    return (function _try(attempt) {
        return self.pool.connection(function (connection) {
            return connection.send(name, params);
        })
        .catch(errors.RiakConnectionError, function (err) {
            self.emit('net:error', err);
            if (attempt < self.options.retries) {
                return _try(++attempt);
            }

            throw err;
        });
    }(0));
};

Client.prototype.init = function () {
    return this._request('RpbSetClientIdReq', {
        client_id: this.options.clientId
    });
};

Client.prototype.end = function () {
    var self = this;

    return Promise.try(function () {
        return self.pool.close();
    });
};

/**
 * Get server info
 *
 * @return {Promise}
 */
Client.prototype.getServerInfo = function () {
    return this._request('RpbGetServerInfoReq');
};

Client.prototype.ping = function () {
    return this._request('RpbPingReq');
};

/**
 * Get value from bucket
 *
 * @param  {Object} params { bucket, key, r, pr, basic_quorum, notfound_ok ...}
 * @return {Promise}
 */
Client.prototype.get = function (params) {
    var self = this;

    return self._request('RpbGetReq', params).then(function (result) {
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

        return self._request('RpbPutReq', params).then(function (result) {
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
    if (typeof params.vclock === 'string') {
        params.vclock = new Buffer(params.vclock, 'base64');
    }

    return this._request('RpbDelReq', params);
};

/**
 * List buckets
 * @param  {Object} params { timeout, type }
 * @return {Promise}
 */
Client.prototype.listBuckets = function (params) {
    params = params || {};
    params.stream = true;

    return this._request('RpbListBucketsReq', params).then(function (results) {
        return _.flatMap(results, 'buckets');
    });
};

/**
 * List keys in a bucket
 * @param  {Object} params { bucket, timeout, type }
 * @return {Promise}
 */
Client.prototype.listKeys = function (params) {
    return this._request('RpbListKeysReq', params).then(function (results) {
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
    params = params || {};
    params.stream = true;

    return this._request('RpbIndexReq', params).then(function (results) {
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

        return self._request('RpbMapRedReq', params).then(function (results) {
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

    return self._request('RpbGetReq', {
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
        return self._request('RpbPutReq', params);
    });
};

Client.prototype.updateCounter = function (params) {
    return this._request('RpbCounterUpdateReq', params).then(function (result) {
        if (result !== null) {
            return result.value;
        }
        return null;
    });
};

Client.prototype.getCounter = function (params) {
    return this._request('RpbCounterGetReq', params).then(function (result) {
        if (result !== null) {
            return result.value;
        }
        return null;
    });
};

Client.prototype.getBucket = function (params) {
    return this._request('RpbGetBucketReq', params).then(function (result) {
        ['pr', 'r', 'w', 'pw', 'dw', 'rw'].forEach(function (p) {
            result.props[p] = riakQuorumValues[result.props[p]] || result.props[p];
        });
        return result;
    });
};

Client.prototype.setBucket = function (params) {
    ['pr', 'r', 'w', 'pw', 'dw', 'rw'].forEach(function (p) {
        if (params && params.props && (typeof params.props[p] === 'string')) {
            params.props[p] = riakQuorumValues[params.props[p]] || params.props[p];
        }
    });

    return this._request('RpbSetBucketReq', params);
};

Client.prototype.resetBucket = function (params) {
    return this._request('RpbResetBucketReq', params);
};

Client.prototype.getBucketType = function (params) {
    return this._request('RpbGetBucketTypeReq', params).then(function (result) {
        ['pr', 'r', 'w', 'pw', 'dw', 'rw'].forEach(function (p) {
            result.props[p] = riakQuorumValues[result.props[p]] || result.props[p];
        });
        return result;
    });
};

Client.prototype.setBucketType = function (params) {
    ['pr', 'r', 'w', 'pw', 'dw', 'rw'].forEach(function (p) {
        if (params && params.props && (typeof params.props[p] === 'string')) {
            params.props[p] = riakQuorumValues[params.props[p]] || params.props[p];
        }
    });

    return this._request('RpbSetBucketTypeReq', params);
};

Client.prototype.getSearchIndex = function (params) {
    return this._request('RpbYokozunaIndexGetReq', params);
};

Client.prototype.putSearchIndex = function (params) {
    return this._request('RpbYokozunaIndexPutReq', params);
};

Client.prototype.delSearchIndex = function (params) {
    return this._request('RpbYokozunaIndexDeleteReq', params);
};

Client.prototype.getSearchSchema = function (params) {
    return this._request('RpbYokozunaSchemaGetReq', params);
};

Client.prototype.putSearchSchema = function (params) {
    return this._request('RpbYokozunaSchemaPutReq', params);
};

Client.prototype.search = function (params) {
    return this._request('RpbSearchQueryReq', params).then(function (result) {
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

Client.prototype.dtFetch = function (params) {
    return this._request('DtFetchReq', params);
};

Client.prototype.dtUpdate = function (params) {
    return this._request('DtUpdateReq', params);
};
