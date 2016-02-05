'use strict';

function RiakError(code, message) {
    Error.captureStackTrace(this, this.constructor);

    this.name = this.constructor.name;
    this.code = code;
    this.message = message || 'Error';
}

function ConnectionError(connection, message) {
    // Error.captureStackTrace(this, this.constructor);

    this.name = this.constructor.name;
    if (connection) {
        this.host = connection.host();
        this.port = connection.port();
    }
    this.message = message || 'Error';
}

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
