'use strict';

var DataType     = require('./base');
var SetField = require('./fields/set');
var util         = require('util');

function RSet(client, params) {
    DataType.call(this, client, params, SetField);
}

module.exports = RSet;

util.inherits(RSet, DataType);

RSet.prototype.add = function () {
    this._field.add.apply(this._field, arguments);
    return this;
};

RSet.prototype.remove = function () {
    this._field.remove.apply(this._field, arguments);
    return this;
};
