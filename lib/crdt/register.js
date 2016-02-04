'use strict';

var DataType     = require('./base');
var util         = require('util');

function Register(_client, params) {
    this._value = null;

    DataType.call(this, -1, null, params);

    if (this.params.strings === undefined) {
        this.params.strings = true; // convert set values to strings
    }
}

module.exports = Register;

util.inherits(Register, DataType);

Register.prototype.set = function (v) {
    this._value = v;
    return this;
};

// private methods
Register.prototype._op = function () {
    return {
        register_op: this._value
    };
};

Register.prototype._reset = function () {
};

Register.prototype.value = function () {
    return this.params.strings ? this._value.toString('utf8') : this._value;
};

Register.prototype._setValue = function (v) {
    this._value = v.register_value;
};
