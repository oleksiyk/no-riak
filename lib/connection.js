'use strict';

var net             = require('net');
var Promise         = require('bluebird');
var ConnectionError = require('./errors').ConnectionError;
var _               = require('lodash');

function Connection(options) {
    this.options = _.defaults(options || {}, {
        port: 8087,
        host: '127.0.0.1',
        connectionTimeout: 3000,
        initialBufferSize: 256 * 1024
    });

    this.connected = false;
    this.buffer = new Buffer(this.options.initialBufferSize);
    this.offset = 0;

    this.id = _.uniqueId();
}

module.exports = Connection;

Connection.prototype.connect = function () {
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
                reject(new ConnectionError(self, 'Connection timeout to ' + self.options.host + ':' + self.options.port));
            }, self.options.connectionTimeout);
        }),
        new Promise(function (resolve, reject) {
            if (self.socket) {
                self.socket.destroy();
            }

            self.socket = new net.Socket();
            self.socket.on('end', function () {
                self._disconnect(new ConnectionError(self, 'Riak server [' + self.options.host + ':' + self.options.port + '] has closed connection'));
            });
            self.socket.on('error', function (err) {
                var _err = new ConnectionError(self, err.toString());
                reject(_err);
                self._disconnect(_err);
            });
            self.socket.on('data', self._receive.bind(self));

            self.socket.connect(self.options.port, self.options.host, function () {
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

Connection.prototype._disconnect = function (err) {
    if (!this.connected) {
        return;
    }

    this.socket.end();
    this.connected = false;

    if (this.task) {
        this.task.reject(err);
    }
};

Connection.prototype._growBuffer = function (newLength) {
    var _b = new Buffer(newLength);
    this.buffer.copy(_b, 0, 0, this.offset);
    this.buffer = _b;
};

Connection.prototype.close = function () {
    this._disconnect({ _connection_closed: true });
};

/**
 * Send a request to Riak
 */
Connection.prototype.send = function (data, task) {
    var self = this;

    self.task = task;

    function _send() {
        return new Promise(function (resolve) {
            self.socket.write(data, resolve);
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

    if (this.offset) {
        if (this.buffer.length < data.length + this.offset) {
            this._growBuffer(data.length + this.offset);
        }
        data.copy(this.buffer, this.offset);
        this.offset += data.length;
        data = this.buffer.slice(0, this.offset);
    }

    length = data.length < 4 ? 0 : data.readInt32BE(0);

    if (data.length < 4 + length) {
        if (this.offset === 0) {
            if (this.buffer.length < 4 + length) {
                this._growBuffer(4 + length);
            }
            data.copy(this.buffer);
            this.offset += data.length;
        }
        return;
    }

    this.offset = 0;

    this.task.process(data.slice(0, length + 4)); // send it with message length

    if (data.length > 4 + length) {
        this._receive(data.slice(length + 4));
    }
};
