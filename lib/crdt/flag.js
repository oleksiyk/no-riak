'use strict';

var DataType     = require('./base');
var util         = require('util');

function Flag() {
    this._value = false;

    DataType.call(this);
}

module.exports = Flag;

util.inherits(Flag, DataType);

Flag.prototype.enable = function () {
    this._value = true;
    return this;
};

Flag.prototype.disable = function () {
    this._value = false;
    return this;
};

// private methods
Flag.prototype._op = function () {
    return {
        flag_op: this._value === true ? 1 : 2
    };
};

Flag.prototype._reset = function () {
};

Flag.prototype.value = function () {
    return this._value;
};

Flag.prototype._setValue = function (v) {
    this._value = v.flag_value;
};
