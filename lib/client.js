'use strict';

// var Promise  = require('bluebird');
var Pool     = require('./pool');
// var errors   = require('./errors');
var _        = require('lodash');

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
 * Retrieve value from bucket
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

Client.prototype.listKeys = function (params) {
    var self = this;

    return self.pool.send('RpbListKeysReq', params).then(function (results) {
        return _.flatMap(results, 'keys');
    });
};
