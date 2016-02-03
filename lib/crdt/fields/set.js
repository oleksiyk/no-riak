'use strict';

var _ = require('lodash');
var _value;

function SetField(dtValue) {
    this._initial = [];

    if (dtValue) {
        this._initial = dtValue.set_value;
    }

    this.adds = [];
    this.removes = [];
}

module.exports = SetField;

SetField.type = 2;

SetField.prototype._op = function () {
    return {
        set_op: {
            adds: this.adds,
            removes: this.removes
        }
    };
};

_value = _.memoize(function (self) {
    return _(self._initial).union(self.adds).pullAll(self.removes).value();
});

SetField.prototype.add = function () {
    Array.prototype.push.apply(this.adds, arguments);
    _value.cache.delete(this);
};

SetField.prototype.remove = function () {
    Array.prototype.push.apply(this.removes, arguments);
    _value.cache.delete(this);
};

SetField.prototype.reset = function () {
    this.adds = [];
    this.removes = [];
    _value.cache.delete(this);
};

Object.defineProperty(SetField.prototype, 'value', {
    enumerable: true,
    get: function () {
        return _value(this);
    },
    set: function (v) {
        this._initial = v.set_value;
        _value.cache.delete(this);
    }
});
