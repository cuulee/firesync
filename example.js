/* jshint camelcase: false */

/*!
 * deps
 */

var elasticsearch = require('elasticsearch'),
    through = require('through2'),
    Firenext = require('firenext'),
    sync = require('./index');

/*!
 * globals
 */

var FirebaseReadStream = sync.FirebaseReadStream,
    FirebaseEventStream = sync.FirebaseEventStream,
    ElasticBulkStream = sync.ElasticBulkStream,
    LogStream = sync.LogStream,
    ref, client, fbEventStream, esBulkStream;

// create firebase and elasticsearch clients
ref = new Firenext('https://jogabo-test.firebaseio.com/jobs');
client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'info'
});

// create data streams
fbReadStream = new FirebaseReadStream(ref.orderByPriority());
fbEventStream = new FirebaseEventStream(ref);
esBulkStream = new ElasticBulkStream(client, {
  index: 'firequeue'
});

// example 1
// stream from firebaseEventStream
// push to Elasticsearch
// print to console
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
  .pipe(new LogStream(function(child) {
    console.log(child[0]);
  }));

// example 2
// stream from firebaseReadStream
// print to console
fbReadStream
  .pipe(new LogStream(function(child) {
    console.log(child.key(), child.val().name);
  }));

// reset
// curl -XDELETE http://localhost:9200/firequeue
// curl -XPOST http://localhost:9200/firequeue
