'use strict';

var RiakError = function (code, message) {
    Error.call(this);
    Error.captureStackTrace(this, this.constructor);

    this.name = 'RiakError';
    this.code = code;
    this.message = message || 'Error';
};

var ConnectionError = function (connection, message) {
    Error.call(this);
    // Error.captureStackTrace(this, this.constructor);

    this.name = 'ConnectionError';
    this.host = connection.options.host;
    this.port = connection.options.port;
    this.message = message || 'Error';
};

exports.RiakError = RiakError;
exports.ConnectionError = ConnectionError;

RiakError.prototype = Object.create(Error.prototype);
RiakError.prototype.constructor = RiakError;

RiakError.prototype.toJSON = function () {
    return {
        name: this.name,
        code: this.code,
        message: this.message
    };
};

RiakError.prototype.toString = function () {
    return this.name + ': ' + this.code + ': ' + this.message;
};


ConnectionError.prototype = Object.create(Error.prototype);
ConnectionError.prototype.constructor = ConnectionError;

ConnectionError.prototype.toJSON = function () {
    return {
        name: this.name,
        host: this.host,
        port: this.port,
        message: this.message
    };
};

ConnectionError.prototype.toString = function () {
    return this.name + ' (' + this.host + ':' + this.port + '): ' + this.message;
};
