'use strict';

var Client = require('./lib/client');
var errors = require('./lib/errors');
var crdt   = require('./lib/crdt');
var _      = require('lodash');

module.exports = _.merge({ Client: Client }, errors, { CRDT: crdt });
