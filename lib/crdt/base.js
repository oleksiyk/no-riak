'use strict';

var _       = require('lodash');
var Promise = require('bluebird');

function DataType(client, params, FieldConstructor) {
    this.client = client;
    this.params = params;
    this._field = new FieldConstructor();
    this.FieldConstructor = FieldConstructor;

    this.context = undefined;
}

module.exports = DataType;

DataType.prototype.load = function () {
    var self = this;

    return self.client.dtFetch(self.params).then(function (response) {
        if (response.type !== self.FieldConstructor.type) {
            throw new Error(self.params.bucket + '/' + self.params.key + ' is of wrong type, expected ' + self.FieldConstructor.type + ', got ' + response.type);
        }
        self.context = response.context;

        if (response.value) {
            self._field.value = response.value;
        }

        return self;
    });
};

DataType.prototype.save = function () {
    var self = this;

    return self.client.dtUpdate(_.merge(self.params, {
        op: self._field._op(),
        context: self.context
    }))
    .then(function (response) {
        if (response && response.key) {
            self.params.key = response.key;
        }

        self._field.reset();

        return self;
    });
};

DataType.prototype.key = function () {
    return this.params.key;
};

DataType.prototype.value = function () {
    var self = this;

    if (!self.params.key) {
        return Promise.resolve(self._field.value);
    }

    return self.load().then(function () {
        return self._field.value;
    });
};
