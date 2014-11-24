/*!
 * deps
 */

var elasticsearch = require('elasticsearch'),
    Firebase = require('firebase'),
    sync = require('.');

/*!
 * globals
 */

var FirebaseChildStream = sync.FirebaseChildStream,
    ElasticBulkStream = sync.ElasticBulkStream,
    ref, client, fbEventStream, esBulkStream, logStream;

// create firebase and elasticsearch clients
ref = new Firebase('https://mydb.firebaseio.com/jobs');
client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'info'
});

// create data streams
logStream = new LogStream();
fbEventStream = new FirebaseChildStream(ref);
esBulkStream = new ElasticBulkStream(client, {
  index: 'firequeue',
  type: 'job'
});

// start indexing
fbEventStream
  .pipe(esBulkStream)
  .pipe(logStream);

// reset
// curl -XDELETE http://localhost:9200/firequeue
// curl -XPOST http://localhost:9200/firequeue
