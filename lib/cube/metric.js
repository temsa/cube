// TODO use expression ids or hashes for more compact storage
var parser = require("./metric-expression"),
    tiers = require("./tiers"),
    types = require("./types"),
    reduces = require("./reduces"),
    event = require("./event"),
    mongodb = require("mongodb"),
    ObjectID = mongodb.ObjectID,
    crypto = require("crypto");

var metric_fields = {v: 1},
    metric_options = {sort: {"_id.t": 1}, batchSize: 1000},
    event_options = {sort: {_id: 1}, batchSize: 1000, timeout: false},
    limitMax = 1e4;

// Regexp to remove whitespaces other than inside nested strings
// Known limitations :
//   - Fails when another token (' or ") is inside a string (ie. "'" or '"')
var whitespaceRemover = / *([^ \"']*) *((\"|')(?:[^\\\\\"']|\\\\.)*(\"|'))?/g;
function shasum (string) {
  var sha512 = crypto.createHash("sha512");
  sha512.update(string);
  return sha512.digest("base64");
}

// Ease debugging of metrics after they have been hashed by storing equivalencies.
var hashesColl;
function rememberHashing(db) {
  function memorize (original, hashed) {
    if (!hashesColl) return;
    hashesColl.update({ original: original }, { original: original, hashed: hashed }, { upsert: true, w: 0 });
  }

  if (!hashesColl) {
    db.collection('formulaHashes', function (err, collection) {
      if (err) return; // This is not critical, better luck next time
      hashesColl = collection;
    });
  }

  return memorize;
}

function hashExpression (expression, memorize) {
  if (expression.source) {
    // Workaround for https://jira.mongodb.org/browse/SERVER-4271
    // Because expression is in the sharding key, heavy formulas are not persisted
    // So we change the expression to be stripped from spaces, then put it in the form
    // "length sha512"
    var original = expression.source;
    expression.source = expression.source.replace(whitespaceRemover, '$1$2');
    expression.source = expression.source.length + " " + shasum(expression.source);
    memorize(original, expression.source);
  } else {
    if (expression.left) {
      hashExpression(expression.left, memorize);
    }
    if (expression.right) {
      hashExpression(expression.right, memorize);
    }
  }
}

// When streaming metrics, we should allow a delay for events to arrive, or else
// we risk skipping events that arrive after their event time.
var streamDelayDefault = 7000,
    streamInterval = 1000;

// Query for metrics.
exports.getter = function(db) {
  var collection = types(db),
      Double = db.bson_serializer.Double,
      queueByName = {},
      meta = event.putter(db),
      streamsBySource = {};

  function getter(request, callback) {

    // Provide default start and stop times for recent events.
    // If the limit is not specified, or too big, use the maximum limit.
    var stream = request.stop === undefined,
        limit = +request.limit < limitMax ? +request.limit : limitMax,
        step = +request.step ? +request.step : 1e4,
        stop = (request.stop !== undefined) ? new Date(request.stop) : new Date(Date.now() - streamDelayDefault),
        start = (request.start !== undefined) ? new Date(request.start) : new Date(0),
        id = request.id;

    // If the time between start and stop is too long, then bring the start time
    // forward so that only the most recent results are returned. This is only
    // approximate in the case of months, but why would you want to return
    // exactly ten thousand months? Don't rely on exact limits!
    if ((stop - start) / step > limit) start = new Date(stop - step * limit);

    // Parse the expression.
    var expression;
    try {
      expression = parser.parse(request.expression);
      hashExpression(expression, rememberHashing(db));
    } catch (e) {
      return callback({error: "invalid expression", id: id}), -1;
    }

    // Round start and stop to the appropriate time step.
    var tier = tiers[step];
    if (!tier) return callback({error: "invalid step", id: id}), -1;
    start = tier.floor(start);
    stop = tier[ stream ? 'floor' : 'ceil' ](stop);

    var filter = { start: start, stop: stop };

    function query(callback) {
      // Compute the request metric!
      measure(expression, filter.start, filter.stop, tier, function(time, value) {
        callback({time: time, value: value, id: id});
      });
    }

    if (stream) {
      var streamKey = request.expression.replace(/\s+/g, '') + '#' + tier.key,
        streams = streamsBySource[streamKey],
        metrics = [];

      // A poll function already exists for this expression/tier couple,
      // get the previously computed data and add ourselve to the stream.
      if (streams) {
        filter.stop = streams.start;
        streams.active.push({
          id: id,
          callback: callback
        });
        query(function(metric) {
          if (metric.time < filter.stop) {
            metrics.push(metric);
          } else {
            metrics.sort(chronological).forEach(function (metric) {
              callback(metric);
            });
          }
        });
      }

      // No poll function exist for this expression/tier couple, let's create a new one.
      else
      {
        streams = streamsBySource[streamKey] = {
          start: start,
          stop: stop,
          active: [{
            id: id,
            callback: callback
          }]
        };

        (function poll() {
          query(function (metric) {
            if (metric.time < filter.stop) {
              metrics.push(metric);
            } else {
              metrics.sort(chronological).forEach(function (metric) {
                streams.active.forEach(function (request) {
                  if (!request.callback.closed) {
                    request.callback({
                      time: metric.time,
                      value: metric.value,
                      id: request.id
                    });
                  }
                });
              });

              metrics.length = 0;
              streams.active = streams.active.filter(open);

              if (!streams.active.length) {
                delete streamsBySource[streamKey];
                return;
              }

              // Previous stops becomes our new start, the new stop gets a tier added,
              // then we ask for this computation on the next stop time + delay.
              // Note that the timeout might be negative in case of overload,
              // though that should not matter much.
              filter.start = streams.start = streams.stop;
              filter.stop = streams.stop = new Date (+streams.stop + tier.key);
              setTimeout(poll, +filter.stop + streamDelayDefault - Date.now());
            }
          });
        })();
      }
    }

    // For non-streaming queries, just send the single batch!
    else query(callback);
  }

  // Computes the metric for the given expression for the time interval from
  // start (inclusive) to stop (exclusive). The time granularity is determined
  // by the specified tier, such as daily or hourly. The callback is invoked
  // repeatedly for each metric value, being passed two arguments: the time and
  // the value. The values may be out of order due to partial cache hits.
  function measure(expression, start, stop, tier, callback) {
    (expression.op ? binary : expression.type ? unary : constant)(expression, start, stop, tier, callback);
  }

  // Computes a constant expression;
  function constant(expression, start, stop, tier, callback) {
    var value = expression.value();
    while (start < stop) {
      callback(start, value);
      start = tier.step(start);
    }
    callback(stop);
  }

  // Serializes a unary expression for computation.
  function unary(expression, start, stop, tier, callback) {
    var remaining = 0,
        time0 = Date.now(),
        time = start,
        name = expression.source,
        queue = queueByName[name],
        step = tier.key;

    // Compute the expected number of values.
    while (time < stop) ++remaining, time = tier.step(time);

    // If no results were requested, return immediately.
    if (!remaining) return callback(stop);

    // Add this task to the appropriate queue.
    if (queue) queue.next = task;
    else process.nextTick(task);
    queueByName[name] = task;

    function task() {
      findOrComputeUnary(expression, start, stop, tier, function(time, value) {
        callback(time, value);
        if (!--remaining) {
          callback(stop);
          if (task.next) process.nextTick(task.next);
          else delete queueByName[name];

          // Record how long it took us to compute as an event!
          var time1 = Date.now();
          meta({
            type: "cube_compute",
            time: time1,
            data: {
              expression: expression.source,
              ms: time1 - time0
            }
          });
        }
      });
    }
  }

  // Finds or computes a unary (primary) expression.
  function findOrComputeUnary(expression, start, stop, tier, callback) {
    var name = expression.type,
        type = collection(name),
        map = expression.value,
        reduce = reduces[expression.reduce],
        finalize = reduces.finalize[expression.reduce] || function identity(v){return v;},
        filter = {_id: {}},
        fields = {_id: 1};

    // Copy any expression filters into the query object.
    expression.filter(filter);

    // Request any needed fields.
    expression.fields(fields);

    find(start, stop, tier, function(t,v){ var f = finalize(v); callback(t,f); });

    // The metric is computed recursively, reusing the above variables.
    function find(start, stop, tier, callback) {
      var compute = tier.next && reduce.pyramidal ? computePyramidal : computeFlat,
          step = tier.key;

      // Query for the desired metric in the cache.
      type.metrics.find({
        i: false,
        "_id.e": expression.source,
        "_id.l": tier.key,
        "_id.t": {
          $gte: start,
          $lt: stop
        }
      }, metric_fields, metric_options, foundMetrics);

      // Immediately report back whatever we have. If any values are missing,
      // merge them into contiguous intervals and asynchronously compute them.
      function foundMetrics(error, cursor) {
        handle(error);
        var time = start;
        cursor.each(function(error, row) {
          handle(error);
          if (row) {
            callback(row._id.t, row.v);
            if (time < row._id.t) compute(time, row._id.t);
            time = tier.step(row._id.t);
          } else {
            if (time < stop) compute(time, stop);
          }
        });
      }

      // Group metrics from the next tier.
      function computePyramidal(start, stop) {
        var bins = {};
        find(start, stop, tier.next, function(time, value) {
          var bin = bins[time = tier.floor(time)] || (bins[time] = {size: tier.size(time), values: []});
          if (bin.values.push(value) === bin.size) {
            save(time, reduce(bin.values));
            delete bins[time];
          }
        });
      }

      // Group raw events. Unlike the pyramidal computation, here we can control
      // the order in which rows are returned from the database. Thus, we know
      // when we've seen all of the events for a given time interval.
      function computeFlat(start, stop) {
        filter._id.$gte = ObjectID.createFromTime(start/1000);
        filter._id.$lt = ObjectID.createFromTime(stop/1000);
        type.events.find(filter, fields, event_options, function(error, cursor) {
          handle(error);
          var time = start, values = [], computes = [];
          cursor.each(function(error, row) {
            handle(error);
            if (row) {
              var then = tier.floor(row._id.getTimestamp());
              if (time < then) {
                computes.push({ time: time, value: values.length ? reduce(values) : reduce.empty });
                while ((time = tier.step(time)) < then) computes.push({ time: time, value: reduce.empty });
                values = [map(row)];
              } else {
                values.push(map(row));
              }
            } else {
              computes.push({ time: time, value: values.length ? reduce(values) : reduce.empty });
              while ((time = tier.step(time)) < stop) computes.push({ time: time, value: reduce.empty });
              saveBulk(computes);
            }
          });
        });
      }

      function save(time, value) {
        callback(time, value);
        if (value) {
          type.metrics.save({
            _id: {
              e: expression.source,
              l: tier.key,
              t: time
            },
            i: false,
            v: value
          }, handle);
        }
      }

      function saveBulk (computes) {
        var rows = [];
        computes.forEach(function (compute) {
          callback(compute.time, compute.value);
          rows.push({
            _id: {
              e: expression.source,
              l: tier.key,
              t: compute.time
            },
            i: false,
            v: compute.value
          });
        });
        type.metrics.insert(rows, { continueOnError: true }, handle);
      }
    }
  }

  // Computes a binary expression by merging two subexpressions.
  function binary(expression, start, stop, tier, callback) {
    var left = {}, right = {};

    measure(expression.left, start, stop, tier, function(t, l) {
      if (t in right) {
        callback(t, t < stop ? expression.op(l, right[t]) : l);
        delete right[t];
      } else {
        left[t] = l;
      }
    });

    measure(expression.right, start, stop, tier, function(t, r) {
      if (t in left) {
        callback(t, t < stop ? expression.op(left[t], r) : r);
        delete left[t];
      } else {
        right[t] = r;
      }
    });
  }

  getter.close = function(callback) {
    callback.closed = true;
  };

  return getter;
};

function handle(error) {
  if (error) throw error;
}

function open(request) {
  return !request.callback.closed;
}

function chronological(a, b) {
  return a.time - b.time;
}
