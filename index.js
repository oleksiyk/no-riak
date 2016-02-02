'use strict';

var Client = require('./lib/client');
var errors = require('./lib/errors');
var _      = require('lodash');

module.exports = _.merge(Client, errors);
