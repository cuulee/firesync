/*!
 * module deps
 */

var stream = require('stream'),
    debug = require('debug')('firesync'),
    util = require('util');

/*!
 * globals
 */

var Readable = stream.Readable,
    Writable = stream.Writable,
    Transform = stream.Transform,
    noop = function() {};

/**
 * FirebaseReadStream constructor
 * created a Readable that read child at a firebase location and stream children
 *
 * @param { Firenext Query } query
 * @param { {pageSize = 100} } opts
 */

function FirebaseReadStream(query, opts) {
  if (!opts) opts = {};

  Readable.call(this, { objectMode: true });
  this.query = query;
  this.pageSize = opts.pageSize || 100;
  this.max = opts.max || Infinity;
  this.count = 0;
  this.marker = null;
  this.reading = false;
}

/*!
 * extend readable
 */

util.inherits(FirebaseReadStream, Readable);

/**
 * FirebaseReadStream internal _read
 * @see http://nodejs.org/api/stream.html#stream_readable_read_size_1
 */

FirebaseReadStream.prototype._read = function() {
  var stream = this,
      query = this.query;

  if (stream.reading) return;
  stream.reading = true;

  if (this.marker) {
    query = query
      .startAt(this.marker.getPriority(), this.marker.key())
      .limitToFirst(this.pageSize + 1);
  } else {
    query = query
      .limitToFirst(this.pageSize);
  }

  query
    .exec()
    .then(function(data) {
      var pushResult, newMarker;

      data.forEach(function(child) {
        if (!stream.marker || stream.marker.key() !== child.key()) {
          pushResult = stream.push(child);
          newMarker = child;
          stream.count++;
        }
      });

      // reset reading flag
      stream.reading = false;

      // check if there is more items
      if (!newMarker) {
        stream.push(null);

      // check if we reached max
      } else if (stream.count >= stream.max) {
        stream.push(null);

      // read more if push returned true
      } else if (pushResult) {
        stream.marker = newMarker;
        stream._read();
      }
    })
    .catch(function(error) {
      stream.emit('error', error);
    });
};

/**
 * FirebaseEventStream constructor
 * created a Readable that listen to firebase events and stream children
 *
 * @param {Object} opts
 */

function FirebaseEventStream(ref, opts) {
  Readable.call(this, { objectMode: true });
  this.ref = ref;
  this.events = (opts || {}).events || [
    'child_added',
    'child_removed',
    'child_changed'
  ];
}

/*!
 * extend readable
 */

util.inherits(FirebaseEventStream, Readable);

/**
 * FirebaseEventStream internal _read
 * @see http://nodejs.org/api/stream.html#stream_readable_read_size_1
 */

FirebaseEventStream.prototype._read = function() {
  var stream = this;

  // Limit _read to call only once per response
  if (this.listening) return;
  this.listening = true;

  function childEvent(ev) {
    return function(child) {
      stream.push({ event: ev, child: child });
    };
  }

  this.events.forEach(function(ev) {
    stream.ref.on(ev, childEvent(ev));
  });
};

/**
 * ElasticBulkStream constructor
 * @param {Object} opts
 */

function ElasticBulkStream(client, opts) {
  Transform.call(this, { objectMode: true });
  this.client = client;
  this.bodyMaxSize = opts.bodyMaxSize || 10;
  this.flushInterval = opts.flushInterval || 1000;
  this.index = opts.index;
  this.body = [];
}

/*!
 * extend transform
 */

util.inherits(ElasticBulkStream, Transform);

/**
 * bulk indexer stream _transform implementation
 * add doc to the internal body buffer and flush every `bodyMaxSize`
 *
 * @see http://nodejs.org/api/stream.html#stream_transform_transform_chunk_encoding_callback
 */

ElasticBulkStream.prototype._transform = function(chunk, encoding, callback) {
  this.body.push.apply(this.body, chunk);
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
    index: this.index
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

function LogStream(logfn) {
  Writable.call(this, { objectMode: true });
  this.log = logfn || function(chunk) {
    debug(chunk);
  };
}

util.inherits(LogStream, Writable);

LogStream.prototype._write = function(chunk, encoding, callback) {
  this.log(chunk);
  callback();
};

/*!
 * exports
 */

module.exports.LogStream = LogStream;
module.exports.ElasticBulkStream = ElasticBulkStream;
module.exports.FirebaseEventStream = FirebaseEventStream;
module.exports.FirebaseReadStream = FirebaseReadStream;