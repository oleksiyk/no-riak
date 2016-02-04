'use strict';

var _ = require('lodash');

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

    return self.client.dtUpdate(_.mergeWith({}, self.params, {
        op: self._op(),
        context: self.context,
        return_body: true
    }, function (_a, b) {if (b instanceof Buffer) {return b;}})) // see https://github.com/lodash/lodash/issues/1940
    .then(function (response) {
        if (response && response.key) {
            self.params.key = response.key;
        }

        self._setValue(response);

        if (response.context) {
            self.context = response.context;
        }

        self._reset();

        return self;
    });
};

DataType.prototype.key = function () {
    return this.params.key;
};

