/*!
 * module deps
 */

var stream = require('stream'),
    util = require('util');

/*!
 * globals
 */

var Readable = stream.Readable,
    Writable = stream.Writable,
    Transform = stream.Transform,
    noop = function() {};

/**
 * FirebaseChildStream constructor
 * created a Readable that listen to firebase events and stream children
 *
 * @param {Object} opts
 */

function FirebaseChildStream(ref) {
  Readable.call(this, { objectMode: true });
  this.ref = ref;
}

/*!
 * extend readable
 */

util.inherits(FirebaseChildStream, Readable);

/**
 * FirebaseChildStream internal _read
 * @see http://nodejs.org/api/stream.html#stream_readable_read_size_1
 */

FirebaseChildStream.prototype._read = function() {
  var stream = this;

  // Limit _read to call only once per response
  if (this.listening) return;
  this.listening = true;

  function indexChild(child) {
    stream.push({ op: 'index', child: child });
  }

  function updateChild(child) {
    stream.push({ op: 'udate', child: child });
  }

  function removeChild(child) {
    stream.push({ op: 'remove', child: child });
  }

  this.ref.on('child_added', indexChild);
  this.ref.on('child_removed', removeChild);
  this.ref.on('child_changed', updateChild);
};

/**
 * ElasticBulkStream constructor
 * @param {Object} opts
 */

function ElasticBulkStream(client, opts) {
  Transform.call(this, { objectMode: true });
  this.client = client;
  this.body = [];
  this.bodyMaxSize = opts.bodyMaxSize || 10;
  this.flushInterval = opts.flushInterval || 1000;
  this.index = opts.index;
  this.type = opts.type;
}

/*!
 * extend transform
 */

util.inherits(ElasticBulkStream, Transform);

/**
 * add a snapshot to the body buffer
 *
 * @param {DataSnapshot} snap
 */

function _addToBulkBody(chunk) {
  var doc = { _id: chunk.child.name() };

  switch(chunk.op) {
    case 'index':
      this.body.push({ index: doc }, chunk.child.val());
      break;
    case 'update':
      this.body.push({ update: doc }, chunk.child.val());
      break;
    case 'remove':
      this.body.push({ delete: doc });
      break;
    default:
      throw new Error('invalid operation ' + doc.op);
  }
}

/**
 * bulk indexer stream _transform implementation
 * add doc to the internal body buffer and flush every `bodyMaxSize`
 *
 * @see http://nodejs.org/api/stream.html#stream_transform_transform_chunk_encoding_callback
 */

ElasticBulkStream.prototype._transform = function(chunk, encoding, callback) {
  _addToBulkBody.call(this, chunk);
  this.push(chunk);

  if (this.body.length >= this.bodyMaxSize) {
    this._flush(callback);
  } else if (!this.timeout) {
    this.timeout = setTimeout(this._flush.bind(this, noop), this.flushInterval);
    callback();
  } else {
    callback();
  }
};

/**
 * flush internal bugger
 * @see http://nodejs.org/api/stream.html#stream_transform_flush_callback
 */

ElasticBulkStream.prototype._flush = function(callback) {

  // reset timeout
  clearTimeout(this.timeout);
  this.timeout = null;

  // do nothing if body is empty
  if (!this.body.length) return callback();

  // process bulk command
  this.client.bulk({
    body: this.body.slice(),
    index: this.index,
    type: this.type
  }, function(err) {
    callback(err);
  });

  // reset body
  this.body = [];
};

/**
 * LogStream constructor
 * created a Writable that print firbase snapshots
 *
 * @param {Object} opts
 */

function LogStream() {
  Writable.call(this, { objectMode: true });
}

util.inherits(LogStream, Writable);

LogStream.prototype._write = function(chunk, encoding, callback) {
  console.log(chunk.op, 'child:', chunk.child.name());
  callback();
};

/*!
 * exports
 */

module.exports.LogStream = LogStream;
module.exports.ElasticBulkStream = ElasticBulkStream;
module.exports.FirebaseChildStream = FirebaseChildStream;
