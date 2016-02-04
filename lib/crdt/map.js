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

RMap.Register = require('./register');

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
    var field = _.chain(this._fields)
        .union(this.updates)
        .pullAllBy(this.removes, 'name')
        .findLast({ name: name })
        .get('value')
        .value();

    if (field && _.find(this._fields, { name: name }) && !_.find(this.updates, { name: name })) { // put it into updates[] (once)
        this.update(name, field);
    }

    return field;
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

RMap.prototype.value = function () {
    return _(this._fields)
        .union(this.updates)
        .pullAllBy(this.removes, 'name')
        .keyBy('name')
        .mapValues(function (val) {
            return val.value.value();
        })
        .value();
};

RMap.prototype._setValue = function (v) {
    var self = this;

    if (v && v.map_value) {
        self._fields = v.map_value.map(function (entry) {
            var value;
            switch (mapFieldTypes[entry.field.type]) {
                case 'COUNTER':
                    value = new Counter();
                    value._setValue(entry);
                    break;
                case 'RSET':
                    value = new RSet(null, { strings: self.params.strings });
                    value._setValue(entry);
                    break;
                case 'RMAP':
                    value = new RMap(null, { strings: self.params.strings });
                    value._setValue(entry);
                    break;
                case 'REGISTER':
                    value = new RMap.Register(null, { strings: self.params.strings });
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
    }
};
