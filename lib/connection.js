'use strict';

var net             = require('net');
var Promise         = require('bluebird');
var ConnectionError = require('./errors').ConnectionError;
var _               = require('lodash');

function Connection(options) {
    options = options || {};
    // options
    this.host = options.host || '127.0.0.1';
    this.port = options.port || 8087;

    // internal state
    this.connected = false;
    this.readBuf = new Buffer(256 * 1024);
    this.readBufPos = 0;

    this.queue = [];

    this.id = _.uniqueId();
}

module.exports = Connection;

Connection.prototype.connect = function (timeout) {
    var self = this;

    if (self.connected) {
        return Promise.resolve();
    }

    if (self.connecting) {
        return self.connecting;
    }

    self.connecting = Promise.race([
        new Promise(function (resolve, reject) {
            setTimeout(function () {
                reject(new ConnectionError(self, 'Connection timeout to ' + self.host + ':' + self.port));
            }, timeout || 3000);
        }),
        new Promise(function (resolve, reject) {
            if (self.socket) {
                self.socket.destroy();
            }

            self.socket = new net.Socket();
            self.socket.on('end', function () {
                self._disconnect(new ConnectionError(self, 'Riak server [' + self.host + ':' + self.port + '] has closed connection'));
            });
            self.socket.on('error', function (err) {
                reject(err);
                self._disconnect(err);
            });
            self.socket.on('data', self._receive.bind(self));

            self.socket.connect(self.port, self.host, function () {
                self.connected = true;
                resolve();
            });
        })
    ])
    .finally(function () {
        self.connecting = false;
    });

    return self.connecting;
};

// Private disconnect method, this is what the 'end' and 'error'
// events call directly to make sure internal state is maintained
Connection.prototype._disconnect = function (err) {
    if (!this.connected) {
        return;
    }

    this.socket.end();
    this.connected = false;

    this.queue.forEach(function (t) {
        t.reject(err);
    });

    this.queue = [];
};

Connection.prototype._growReadBuffer = function (newLength) {
    var _b = new Buffer(newLength);
    this.readBuf.copy(_b, 0, 0, this.readBufPos);
    this.readBuf = _b;
};

Connection.prototype.close = function () {
    this._disconnect({ _connection_closed: true });
};

/**
 * Send a request to Riak
 *
 * @param  {Buffer} data request message
 * @return {Promise}      Promise resolved with a Riak response message
 */
Connection.prototype.send = function (data) {
    var self = this;

    function _send() {
        return new Promise(function (resolve, reject) {
            self.queue.push({
                resolve: resolve,
                reject: reject
            });

            self.socket.write(data);
        });
    }

    if (!self.connected) {
        return self.connect().then(function () {
            return _send();
        });
    }

    return _send();
};

Connection.prototype._receive = function (data) {
    var length;

    if (!this.connected) {
        return;
    }

    if (this.readBufPos) {
        if (this.readBuf.length < data.length + this.readBufPos) {
            this._growReadBuffer(data.length + this.readBufPos);
        }
        data.copy(this.readBuf, this.readBufPos);
        this.readBufPos += data.length;
        data = this.readBuf.slice(0, this.readBufPos);
    }

    length = data.length < 4 ? 0 : data.readInt32BE(0);

    if (data.length < 4 + length) {
        if (this.readBufPos === 0) {
            if (this.readBuf.length < 4 + length) {
                this._growReadBuffer(4 + length);
            }
            data.copy(this.readBuf);
            this.readBufPos += data.length;
        }
        return;
    }

    this.readBufPos = 0;

    this.queue.shift().resolve(data.slice(0, length + 4)); // send it with message length

    if (data.length > 4 + length) {
        this._receive(data.slice(length + 4));
    }
};
