'use strict';

var Protocol  = require('bin-protocol');
var fs        = require('fs');
var path      = require('path');
var _         = require('lodash');
var RiakError = require('./errors').RiakError;

var PROTO_FILES = ['riak.proto', 'riak_dt.proto', 'riak_kv.proto', 'riak_search.proto', 'riak_ts.proto', 'riak_yokozuna.proto'];
var MESSAGES_CSV = 'riak_pb_messages.csv';

var RiakProtocol = Protocol.createProtobufProtocol(_.map(PROTO_FILES, function (file) {
    return fs.readFileSync(path.join(__dirname, '../src', file));
}), { typeSpecificDefaults: false });

var messageCodes = (function () {
    var codes = {};
    var lines = fs.readFileSync(path.join(__dirname, '../src', MESSAGES_CSV), 'utf8').split('\n');
    var i, l, line;
    for (i = 0, l = lines.length; i < l; i++) {
        line = lines[i].split(',');
        if (line.length > 1) {
            codes[line[0]] = line[1];
            codes[line[1]] = Number(line[0]);
        }
    }
    return codes;
})();

/*
    Riak proto uses `bytes` field where `string` is more suitable
    so we will convert `bytes` to `string` on decode
    except for the following Message/fieldPatterns map
 */
var bytesToStringIgnorePatterns = {
    RpbGetResp: [
        /^vclock$/, // vclock will be converted to base64 in Client
        /^content\[\d+\]\.value$/ // don't convert content[*].value
    ]
};

module.exports = RiakProtocol;

RiakProtocol.define('Request', {
    write: function (name, value) {
        var _o1 = this.offset, _o2;
        this.skip(4);
        this.UInt8(messageCodes[name]);
        if (value) {
            this[name](value);
        }
        _o2 = this.offset; this.offset = _o1;
        this.UInt32BE(_o2 - _o1 - 4);
        this.offset = _o2;
    }
});

RiakProtocol.define('Response', {
    read: function () {
        var name;

        this.UInt32BE('length');

        if (this.context.length <= 1) { // some response messages dont have body, ex: RpbSetClientIdResp
            return null;
        }

        this.UInt8('code');
        name = messageCodes[this.context.code];
        if (!name) {
            throw new Error('Unknown message code received: ' + this.context.code);
        }

        this._RiakMessage = name;
        this[name]();

        if (this.context.code === 0) { // RpbErrorResp - error response
            // errcode is always 0, see https://github.com/basho/riak_kv/issues/336
            throw new RiakError(this.context.errcode, this.context.errmsg);
        }

        delete this.context.code;
        delete this.context.length;

        if (_.isEmpty(this.context)) {
            return null;
        }
    }
});

// convert `bytes` to `string` except for those in bytesToStringIgnorePatterns
RiakProtocol.define('bytes', {
    read: function () {
        var ind, _path = this.path.join('.');
        var ignorePatterns = bytesToStringIgnorePatterns[this._RiakMessage];

        // console.log(this._RiakMessage, _path);
        this.UVarint('length');
        if (this.context.length <= 0) {
            return null;
        }
        this.raw('value', this.context.length);

        ind = _.findIndex(ignorePatterns, function (p) {
            return p.test(_path);
        });

        if (ind === -1) {
            return this.context.value.toString('utf8');
        }

        return this.context.value;
    }
});
