'use strict';

var DataType     = require('./base');
var CounterField = require('./fields/counter');
var util         = require('util');

function Counter(client, params) {
    DataType.call(this, client, params, CounterField);
}

module.exports = Counter;

util.inherits(Counter, DataType);

Counter.prototype.increment = function (v) {
    this._field.increment(v);
    return this;
};
