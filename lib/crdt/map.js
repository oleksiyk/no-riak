'use strict';

var DataType = require('./base');
var util     = require('util');
var _        = require('lodash');
var Counter  = require('./counter');
var RSet     = require('./set');

var mapFieldTypes = {
    COUNTER  : 1,
    RSET     : 2,
    REGISTER : 3,
    FLAG     : 4,
    RMAP     : 5,
    1        : 'COUNTER',
    2        : 'RSET',
    3        : 'REGISTER',
    4        : 'FLAG',
    5        : 'RMAP'
};

function RMap(client, params) {
    this._fields = []; // [{field({name, type}), type_value}]
    this.removes = [];
    this.updates = [];

    DataType.call(this, 3, client, params);
}

module.exports = RMap;

util.inherits(RMap, DataType);

RMap.prototype.update = function (name, value) {
    this.updates.push({
        name: name,
        value: value
    });
    return this;
};

RMap.prototype.remove = function (name, constructor) {
    if (this.params.key) { // no reason to send remove operation for new object
        this.removes.push({
            name: name,
            type: mapFieldTypes[constructor.name.toUpperCase()]
        });
    }

    _.pullAllBy(this.updates, [{ name: name }], 'name');

    return this;
};

RMap.prototype.get = function (name) {
    return _(this._fields).union(this.updates).pullAllBy(this.removes, 'name').find({ name: name }).value;
};

RMap.prototype._op = function () {
    return {
        map_op: {
            removes: this.removes,
            updates: this.updates.map(function (u) {
                return _.merge({
                    field: {
                        name: u.name,
                        type: mapFieldTypes[u.value.constructor.name.toUpperCase()]
                    }
                }, u.value._op());
            })
        }
    };
};

RMap.prototype._reset = function () {
    this.removes = [];
    this.updates = [];
};

RMap.prototype._getValue = function () {
    var _value = _(this._fields).union(this.updates).pullAllBy(this.removes, 'name').keyBy('name').mapValues(function (val) {
        return val.value._getValue();
    }).value();

    return _value;
};

RMap.prototype._setValue = function (v) {
    this._fields = v.map_value.map(function (entry) {
        var value;
        switch (mapFieldTypes[entry.field.type]) {
            case 'COUNTER':
                value = new Counter();
                value._setValue(entry);
                break;
            case 'RSET':
                value = new RSet();
                value._setValue(entry);
                break;
            default:
                return undefined;
        }

        return {
            name: entry.field.name,
            value: value
        };
    });
};
