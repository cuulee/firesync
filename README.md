firesync
========


Sync Firebase with elasticsearch

```js
var elasticsearch = require('elasticsearch'),
    through = require('through2'),
    Firebase = require('firebase'),
    sync = require('./index');

/*!
 * globals
 */

var FirebaseChildStream = sync.FirebaseChildStream,
    ElasticBulkStream = sync.ElasticBulkStream,
    LogStream = sync.LogStream,
    ref, client, fbEventStream, esBulkStream;

// create firebase and elasticsearch clients
ref = new Firebase('https://jogabo-test.firebaseio.com/jobs');
client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'info'
});

// create data streams
fbEventStream = new FirebaseChildStream(ref);
esBulkStream = new ElasticBulkStream(client, {
  index: 'firequeue'
});

// start indexing
fbEventStream
  .pipe(through.obj(function(chunk, enc, callback) {
    var op = chunk.op,
        child = chunk.child;
    switch(op) {
      case 'index':
        return callback(null, [
          { index: { _id: child.key(), _type: 'firequeue' } },
          { doc: child.val() }
        ]);
      case 'update':
        return callback(null, [
          { update: { _id: child.key(), _type: 'firequeue' } },
          { doc: child.val(), doc_as_upsert: true }]);
      case 'remove':
        return callback(null, [
          { delete: { _id: child.key(), _type: 'firequeue'  } }
        ]);
      default:
        callback(new Error('invalid operation ' + op));
    }
  }))
  .pipe(esBulkStream)
  .pipe(new LogStream());
```
