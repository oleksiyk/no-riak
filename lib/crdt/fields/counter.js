'use strict';

var Long = require('long');

function CounterField(dtValue) {
    this._value = Long.ZERO;
    if (dtValue) {
        this._value = dtValue.counter_value;
    }
    this.updates = [];
}

module.exports = CounterField;

CounterField.type = 1;

CounterField.prototype.increment = function (v) {
    v = v || new Long(1);

    this.updates.push(v);
};

CounterField.prototype._op = function () {
    return {
        counter_op: {
            increment: this.updates.reduce(function (acc, cur) { return acc.add(cur); }, Long.ZERO)
        }
    };
};

CounterField.prototype.reset = function () {
    this.updates = [];
};

Object.defineProperty(CounterField.prototype, 'value', {
    enumerable: true,
    get: function () {
        return this.updates.reduce(function (acc, cur) { return acc.add(cur); }, this._value);
    },
    set: function (v) {
        this._value = v.counter_value;
    }
});
