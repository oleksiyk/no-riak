'use strict';

// var Promise  = require('bluebird');
var Pool     = require('./pool');
var Protocol = require('./protocol');
// var errors   = require('./errors');
var _        = require('lodash');

function Client(options) {
    var self = this;

    self.options = _.defaultsDeep(options || {}, {
        clientId: 'no-riak-client',
        connectionString: '127.0.0.1:8087',
        autoJSON: true
    });

    self.protocol = new Protocol({
        bufferSize: 256 * 1024,
        resultCopy: true
    });

    self.pool = new Pool(self.options.connectionString, self.options.pool);
}

module.exports = Client;

Client.prototype.init = function () {
    var self = this, buffer;

    buffer = self.protocol.write().Request('RpbSetClientIdReq', {
        client_id: self.options.clientId
    }).result;

    return self.pool.send(buffer).then(function (responseBuffer) {
        return self.protocol.read(responseBuffer).Response().result;
    });
};

Client.prototype.end = function () {
    return self.connection.close();
};

/**
 * Get server info
 *
 * @return {Promise}
 */
Client.prototype.getServerInfo = function () {
    var self = this, buffer;

    buffer = self.protocol.write().Request('RpbGetServerInfoReq').result;

    return self.pool.send(buffer).then(function (responseBuffer) {
        return self.protocol.read(responseBuffer).Response().result;
    });
};

/**
 * Retrieve value from bucket
 *
 * @param  {Object} params { bucket, key, r, pr, basic_quorum, notfound_ok ...}
 * @return {Promise}
 */
Client.prototype.get = function (params) {
    var self = this, buffer;

    buffer = self.protocol.write().Request('RpbGetReq', params).result;

    return self.pool.send(buffer).then(function (responseBuffer) {
        return self.protocol.read(responseBuffer).Response().result;
    })
    .then(function (result) {
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
