'use strict';

var _       = require('lodash');
var Promise = require('bluebird');

function DataType(dataType, client, params) {
    this.dataType = dataType;
    this.client = client;
    this.params = params || {};
    this.context = undefined;
}

module.exports = DataType;

DataType.prototype.load = function () {
    var self = this;

    return self.client.dtFetch(self.params).then(function (response) {
        if (response.type !== self.dataType) {
            throw new Error(self.params.bucket + '/' + self.params.key + ' is of wrong type, expected ' + self.dataType + ', got ' + response.type);
        }
        self.context = response.context;

        if (response.value) {
            self._setValue(response.value);
        }

        return self;
    });
};

DataType.prototype.save = function () {
    var self = this;

    return self.client.dtUpdate(_.merge({}, self.params, {
        op: self._op(),
        context: self.context
    }))
    .then(function (response) {
        if (response && response.key) {
            self.params.key = response.key;
        }

        self._reset();

        return self;
    });
};

DataType.prototype.key = function () {
    return this.params.key;
};

DataType.prototype.value = function () {
    var self = this;
    var args = arguments;

    if (!self.params.key) {
        return Promise.resolve(self._getValue.apply(self, args));
    }

    return self.load().then(function () {
        return self._getValue.apply(self, args);
    });
};
