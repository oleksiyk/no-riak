'use strict';

function RiakError(code, message) {
    Error.captureStackTrace(this, this.constructor);

    this.name = this.constructor.name;
    this.code = code;
    this.message = message || 'Error';
}

function RiakConnectionError(server, message) {
    // Error.captureStackTrace(this, this.constructor);

    this.name = this.constructor.name;
    this.server = server;
    this.message = message || 'Error';
}

exports.RiakError = RiakError;
exports.RiakConnectionError = RiakConnectionError;

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


RiakConnectionError.prototype = Object.create(Error.prototype);
RiakConnectionError.prototype.constructor = RiakConnectionError;

RiakConnectionError.prototype.toJSON = function () {
    return {
        name: this.name,
        server: this.server,
        message: this.message
    };
};

RiakConnectionError.prototype.toString = function () {
    return this.name + ' [' + this.server + ']: ' + this.message;
};
