[![Build Status](https://travis-ci.org/oleksiyk/no-riak.png)](https://travis-ci.org/oleksiyk/no-riak)

# no-riak

__no-riak__ is a [Basho Riak KV](http://basho.com/products/riak-kv/) client for Node.js with easy to use [wrappers over CRDT data types](#crdt-data-types). 
Supports Riak [authentication](#authentication), [conection pooling and balancing](#connection-pooling-and-load-balancing) across multiple servers according to their weight.
All methods will return a [promise](https://github.com/petkaantonov/bluebird)

## Installation

```
npm install no-riak
```

## Basic usage

```javascript
var Riak  = require('no-riak');

var client = new Riak.Client();

return client.put({
    bucket: 'test-bucket',
    key: 'key1',
    content: {
        value: 'hello'
    }
})
.then(function () {
    return client.get({
        bucket: 'test-bucket',
        key: 'key1'
    });
})
.then(function (result) {
    // result => { content:
    //    [ { value: <Buffer 68 65 6c 6c 6f>,
    //        vtag: '7V2EHl2Wh06SCAIl4y7M2Y',
    //        last_mod: 1454584844,
    //        last_mod_usecs: 893098 } ],
    //   vclock: 'a85hYGBgzGDKBVI8ypz/fn5Ie3OPQeizegZTIlMeKwPPBvULfFkA' }

    console.log(result.content[0].value.toString()); // => 'hello'
});

```

### Key/Value operations

- `get(params)` 
- `put(params)`
- `del(params)`
- `update(params)`
- `listKeys(params)`
- `updateCounter(params)`
- `getCounter(params)`
 
### Secondary indexes 

- `index(params)`

Example:

```javascript
var bucket = 'no-riak-test-kv';

return Promise.all([0, 1, 2].map(function (i) {
    return client.put({
        bucket: bucket,
        key: 'key' + i,
        content: {
            value: 'i' + i,
            indexes: [{
                key: 'no-riak-test_bin',
                value: 'indexValue'
            }]
        }
    });
}))
.then(function () {
    return client.index({
        bucket: bucket,
        index: 'no-riak-test_bin',
        qtype: 0,
        max_results: 2,
        key: 'indexValue'
    });
})
.then(function (result) {
    // result => { results: [ 'key0', 'key1' ], continuation: 'g20AAAAEa2V5MQ==' }
    
    // now get rest of search results:
    return client.index({
        bucket: bucket,
        index: 'no-riak-test_bin',
        qtype: 0,
        continuation: result.continuation,
        key: 'indexValue'
    });
})
.then(function (result){
    // result => { results: [ 'key2' ] }
});
```

### Map/Reduce

- `mapReduce()`
 
Example:

```javascript
var bucket = 'no-riak-test-kv';
var keys = [];
return Promise.all([0, 1, 2].map(function (i) {
    keys[i] = 'mr_key' + i;
    return client.put({
        bucket: bucket,
        key: keys[i],
        content: {
            value: {
                num: i + 10
            }
        }
    });
}))
.then(function () {
    return client.mapReduce({
        request: {
            inputs: keys.map(function (k) { return [bucket, k]; }),
            query: [{
                map: { // this phase will return JSON decoded values for each input
                    source: 'function(v) { var d = Riak.mapValuesJson(v)[0]; return [d]; }',
                    language: 'javascript',
                    keep: true
                }
            }, { // this phase will return the `num` property of each value
                reduce: {
                    source: 'function(values) { return values.map(function(v){ return v.num; }); }',
                    language: 'javascript',
                    keep: true
                }
            }, { // this phase will return a sum of all values
                reduce: {
                    module: 'riak_kv_mapreduce',
                    function: 'reduce_sum',
                    language: 'erlang',
                    keep: true
                }
            }]
        }
    });
})
.then(function (results) {
    // [ [ { num: 10 }, { num: 12 }, { num: 11 } ], // thats phase 1 results
    //   [ 10, 12, 11 ], // phase 2 results
    //   [ 33 ] ] // phase 3 results
    
    // each index in results array is an array of results for each map/reduce phase
    // even if phase results were stripped with keep: false
});
```

### Operations on Buckets and Bucket Types

- `listBuckets()`
- `getBucket()`
- `setBucket()`
- `resetBucket()`
- `getBucketType()`
- `setBucketType()`

Example:

```javascript
return client.setBucket({
    bucket: 'some-bucket',
    props: {
        allow_mult: true,
        r: 'all' // possible string values are: one, quorum, all, default
    }
});
```

## Other operations

- `ping()`
- `getServerInfo()`

## Authentication

Enable authentication in Riak, create user, add corresponding grants, example: 

```bash
riak-admin security enable
riak-admin security add-user test password=secret
riak-admin security grant riak_kv.put,riak_kv.get on any to test
riak-admin security add-source test 127.0.0.1/32 password
```

And then simply provide `auth` option when creating Client:

```javascript
var client = new Riak.Client({
    auth: {
        user: 'test',
        password: 'secret'
    }
});
```

All communication will be encrypted over TLS. You can override TLS options:

```javascript
var client = new Riak.Client({
    auth: {
        user: 'test',
        password: 'secret'
    },
    tls: {
        secureProtocol: 'SSLv23_method',
        rejectUnauthorized: false,
        ciphers: 'DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA256:DHE-RSA-AES256-SHA:AES128-SHA256:AES128-SHA:AES256-SHA256:AES256-SHA:RC4-SHA'
    }
});
```

### CRDT Data Types

You can operate on a lower level with [Riak CRDT Data Types](http://docs.basho.com/riak/latest/dev/using/data-types/) with the following methods:

- `dtFetch()`
- `dtUpdate()`

__no-riak__ also provides easy to use wrappers over Map, Set and Counter.

#### Counter

Represents signed 64 bit integer (via [long.js](https://github.com/dcodeIO/long.js))

- `increment(value)` [sync] increment counter value with positive or negative value, returns `this`
- `key()` [sync] get counter key
- `value()` [sync] return counter value
- `load()` [async] load counter value from Riak and return `this` in a Promise
- `save()` [async] save counter to Riak and return `this` in a Promise

```javascript
var Riak = require('no-riak');
var client = new Riak.Client();

var bucket = 'no_riak_test_crdt_counter_bucket';
var bucketType = 'no_riak_test_crdt_counter';

var counter = new Riak.CRDT.Counter(client, {
    bucket: bucket,
    type: bucketType
});

return counter.increment(1).increment(-5).save().call('value')
.then(function (v) {
    // v.toNumber() => -4
});
```

### Set

Represents an array of uniqe opaque Buffer values.

- `key()` [sync] get set key
- `value()` [sync] return set value
- `load()` [async] load set value from Riak and return `this` in a Promise
- `save()` [async] save set to Riak and return `this` in a Promise
- `add(value)` [sync] add new value to set, returns `this`
- `remove(value)` [sync] removes value from the set, returns `this`

Example:

```javascript
var bucket = 'no_riak_test_crdt_set_bucket';
var bucketType = 'no_riak_test_crdt_set';

var set = new Riak.CRDT.Set(client, {
    bucket: bucket,
    type: bucketType
});

return set
    .add('a1', 'a2', 'a3', 'a2', 'a2', 'a3')
    .remove('a1')
    .save()
    .call('value')
    .then(function (v) {
        // v => ['a2', 'a3']
    });
```

By default __no-riak__ will convert set values to strings, if you want to stick with buffers, pass `strings: false` option to Set constructor:

```javascript
var set = new Riak.CRDT.Set(client, {
    bucket: bucket,
    type: bucketType,
    strings: false
});
```

#### Map

Represents a list of name/value pairs. Values can be Counters, Sets, Maps, Registers and Flags.

- `key()` [sync] get map key 
- `value()` [sync] return map value
- `load()` [async] load map value from Riak and return `this` in a Promise
- `save()` [async] save map to Riak and return `this` in a Promise
- `update(name, value)` [sync] add or update new field to map, returns `this`
- `remove(name, type)` [sync] remove existing field from the map. `constructor` is one of the following: `Riak.CRDT.Counter`, `Riak.CRDT.Set`, `Riak.CRDT.Map`, `Riak.CRDT.Map.Register`, `Riak.CRDT.Map.Flag`
- `get(name)` [sync] get Riak.CRDT.* instance for corresponding field `name`. This instance can be used to update the map.

Example:

```javascript
var bucket = 'no_riak_test_crdt_map_bucket';
var bucketType = 'no_riak_test_crdt_map';

var map = new Riak.CRDT.Map(client, {
    bucket: bucket,
    type: bucketType
});

return map
    .update('key1', new Riak.CRDT.Counter().increment(-5))
    .update('key2', new Riak.CRDT.Set().add('a1', 'a2', 'a3').remove('a2'))
    .save()
    .call('value')
    .then(function (v) {
        console.log(v); // => { key1: { low: -5, high: -1, unsigned: false }, key2: [ 'a1', 'a3' ] }
    });        
```

Using `get(name)` to operate on map fields

```javascript
var set;
var map = new Riak.CRDT.Map(client, {
    bucket: bucket,
    type: bucketType
});

map
    .update('key1', new Riak.CRDT.Counter().increment(-5))
    .update('key2', new Riak.CRDT.Set().add('a1', 'a2', 'a3'));
    
set = map.get('key2');
set.remove('a2');

map.save().call('value').then(function (v){
    console.log(v); // => { key1: { low: -5, high: -1, unsigned: false }, key2: [ 'a1', 'a3' ] }
});
```

Using Map.register and Map.Flag:

```javascript
var map = new Riak.CRDT.Map(client, {
    bucket: bucket,
    type: bucketType
});

map
    .update('key1', new Riak.CRDT.Map.Register().set('a1'))
    .update('key2', new Riak.CRDT.Map.Flag().enable())
    .save()
    .call('value')
    .then(function (v){
        console.log(v); // { key1: 'a1', key2: true }
    });
```

Set and Register values in a Map will be by default converted to strings, pass `strings: false` to Map constructor to receive buffers instead:

```javascript
var map = new Riak.CRDT.Map(client, {
    bucket: bucket,
    type: bucketType,
    strings: false
});
```

## Connection pooling and load balancing

__no-riak__ can balance requests across a pool of connections. It can also fill the connections pool according to the list of servers and their corresponding weight:

```javascript
var client = new Riak.Client({
    connectionString: '10.0.1.1:8087:4,10.0.1.2:8087:3,10.0.1.3:8087:2',
    pool: {
        min: 9
    }
});
```

Here, 9 connections will be created when client starts, 4 of which will connect to '10.0.1.1', 3 to '10.0.1.2' and 2 to '10.0.1.3'. When there is a demand for more connections, no-riak will create up to `pool.max` connections and will also split them across servers considering their weight.
