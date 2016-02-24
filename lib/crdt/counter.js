'use strict';

var DataType     = require('./base');
var util         = require('util');
var Long         = require('long');

function Counter(client, params) {
    this._value = Long.ZERO;
    this.updates = [];

    DataType.call(this, 1, client, params);
}

module.exports = Counter;

util.inherits(Counter, DataType);

Counter.prototype.increment = function (v) {
    if (v === undefined) {
        v = new Long(1);
    }

    this.updates.push(v);
    return this;
};

// private methods
Counter.prototype._op = function () {
    return {
        counter_op: {
            increment: this.updates.reduce(function (acc, cur) { return acc.add(cur); }, Long.ZERO)
        }
    };
};

Counter.prototype._reset = function () {
    this.updates = [];
};

Counter.prototype.value = function () {
    return this.updates.reduce(function (acc, cur) { return acc.add(cur); }, this._value);
};

Counter.prototype._setValue = function (v) {
    this._value = v.counter_value;
};
