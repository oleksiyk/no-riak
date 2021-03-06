'use strict';

var DataType = require('./base');
var util     = require('util');
var _        = require('lodash');

function RSet(client, params) {
    this.initial = [];
    this.adds = [];
    this.removes = [];

    DataType.call(this, 2, client, params);

    if (this.params.strings === undefined) {
        this.params.strings = true; // convert set values to strings
    }
}

module.exports = RSet;

util.inherits(RSet, DataType);

RSet.prototype.add = function () {
    Array.prototype.push.apply(this.adds, arguments);
    return this;
};

RSet.prototype.remove = function () {
    Array.prototype.push.apply(this.removes, arguments);
    return this;
};

// private methods
RSet.prototype._op = function () {
    return {
        set_op: {
            adds: this.adds,
            removes: this.removes
        }
    };
};

RSet.prototype._reset = function () {
    this.adds = [];
    this.removes = [];
};

RSet.prototype.value = function () {
    var v = _(this.initial).union(this.adds).pullAll(this.removes);

    if (this.params.strings) {
        return v.map(function (b) {
            return b.toString('utf8');
        }).value();
    }

    return v.value();
};

RSet.prototype._setValue = function (v) {
    this.initial = v.set_value;
};
