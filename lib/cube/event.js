// TODO include the event._id (and define a JSON encoding for ObjectId?)
// TODO allow the event time to change when updating (fix invalidation)
var mongodb = require("mongodb"),
    parser = require("./event-expression"),
    tiers = require("./tiers"),
    types = require("./types"),
    bisect = require("./bisect"),
    configuration = require("./configuration"),
    ObjectID = mongodb.ObjectID;

var type_re = /^[a-z][a-zA-Z0-9_]+$/,
    invalidate = {$set: {i: true}},
    multi = {multi: true},
    metric_options = configuration.get("events-db-metric", {capped: true, size: 1e7, autoIndexId: true});

// When streaming events, we should allow a delay for events to arrive, or else
// we risk skipping events that arrive after their event time. This delay can be
// customized by specifying a `delay` property as part of the request.
var streamDelayDefault = 5000,
    streamInterval = 1000;

// How frequently to invalidate metrics after receiving events.
var invalidateInterval = 5000;

var dummyCallback = function(){};

exports.putter = function(db) {
  var collection = types(db),
      knownByType = {},
      eventsToSaveByType = {},
      timesToInvalidateByTierByType = {};

  function putter(request, callback) {
    callback = callback || dummyCallback;
    if(typeof request !== "object") {
      var json = typeof request !== "undefined" ? JSON.stringify(request) : "undefined";
      return callback({error: "invalid request :"+json}), -1;
    }

    var time = "time" in request ? new Date(request.time) : new Date(),
        type = request.type;

    // Validate the date and type.
    if (!type_re.test(type)) return callback({error: "invalid type"}), -1;
    if (isNaN(time)) return callback({error: "invalid time"}), -1;

    // Generate a Mongo ObjectID based on the event time.
    // If an id is specified, add it to the event data.
    var event = {_id: new ObjectID(time/1000), d: request.data};
    if ("id" in request) event.d.id = request.id;

    // If this is a known event type, save immediately.
    if (type in knownByType) return save(type, event);

    // If someone is already creating the event collection for this new type,
    // then append this event to the queue for later save.
    if (type in eventsToSaveByType) return eventsToSaveByType[type].push(event);

    // Otherwise, it's up to us to see if the collection exists, verify the
    // associated indexes, create the corresponding metrics collection, and save
    // any events that have queued up in the interim!

    // First add the new event to the queue.
    eventsToSaveByType[type] = [event];

    // If the events collection exists, then we assume the metrics & indexes do
    // too. Otherwise, we must create the required collections and indexes. Note
    // that if you want to customize the size of the capped metrics collection,
    // or add custom indexes, you can still do all that by hand.
    db.collectionNames(type + "_events", function(error, names) {
      var events = collection(type).events;
      if (names.length) return saveEvents();

      // Ensure uniqueness of id.
      var eventIndexes = configuration.get("events-db-event-indexes", [{key: {"d.id": 1}, params:{unique: true, sparse: true}}]);
      var metricIndexes = configuration.get("events-db-metric-indexes", [
        {key: {"i": 1, "_id.e": 1, "_id.l": 1, "_id.t": 1}},
        {key: {"i": 1, "_id.l": 1, "_id.t": 1}}
      ]);

      eventIndexes.forEach(function(index){events.ensureIndex(index.key, index.param, handle)});
      //events.ensureIndex({"d.id": 1}, {unique: true, sparse: true},  handle); ///commented beacuase it prevents efficient sharding right now

      // Create a capped collection for metrics. Three indexes are required: one
      // for finding metrics, one (_id) for updating, and one for invalidation.
      db.createCollection(type + "_metrics", metric_options, function(error, metrics) {
        handle(error);

        metricIndexes.forEach(function(index){metrics.ensureIndex(index.key, index.param, handle)});
        saveEvents();
      });

      // Save any pending events to the new collection.
      function saveEvents() {
        knownByType[type] = true;
        eventsToSaveByType[type].forEach(function(event) { save(type, event); });
        delete eventsToSaveByType[type];
      }
    });
  }

  // Save the event of the specified type, and queue invalidation of any cached
  // metrics associated with this event type and time.
  //
  // We don't invalidate the events immediately. This would cause many redundant
  // updates when many events are received simultaneously. Also, having a short
  // delay between saving the event and invalidating the metrics reduces the
  // likelihood of a race condition between when the events are read by the
  // evaluator and when the newly-computed metrics are saved.
  function save(type, event) {
    collection(type).events.save(event, handle);
    queueInvalidation(type, event);
  }

  // Schedule deferred invalidation of metrics for this type.
  // For each type and tier, track the metric times to invalidate.
  // The times are kept in sorted order for bisection.
  function queueInvalidation(type, event) {
    var timesToInvalidateByTier = timesToInvalidateByTierByType[type],
        time = event._id.getTimestamp();
    if (timesToInvalidateByTier) {
      for (var tier in tiers) {
        var tierTimes = timesToInvalidateByTier[tier],
            tierTime = tiers[tier].floor(time),
            i = bisect(tierTimes, tierTime);
        if (i >= tierTimes.length) tierTimes.push(tierTime);
        else if (tierTimes[i] > tierTime) tierTimes.splice(i, 0, tierTime);
      }
    } else {
      timesToInvalidateByTier = timesToInvalidateByTierByType[type] = {};
      for (var tier in tiers) {
        timesToInvalidateByTier[tier] = [tiers[tier].floor(time)];
      }
    }
  }

  // Process any deferred metric invalidations, flushing the queues. Note that
  // the queue (timesToInvalidateByTierByType) is copied-on-write, so while the
  // previous batch of events are being invalidated, new events can arrive.
  setInterval(function() {
    for (var type in timesToInvalidateByTierByType) {
      var metrics = collection(type).metrics,
          timesToInvalidateByTier = timesToInvalidateByTierByType[type];
      for (var tier in tiers) {
        metrics.update({
          i: false,
          "_id.l": +tier,
          "_id.t": {$in: timesToInvalidateByTier[tier]}
        }, invalidate, multi);
      }
    }
    timesToInvalidateByTierByType = {}; // copy-on-write
  }, invalidateInterval);

  return putter;
};

exports.getter = function(db) {
  var collection = types(db),
      streamsBySource = {};

  function getter(request, callback) {
    var stream = !("stop" in request),
        delay = "delay" in request ? +request.delay : streamDelayDefault,
        start = "start" in request ? new Date(request.start) : new Date(0),
        stop = stream ? new Date(Date.now() - delay) : new Date(request.stop),
        id = "id" in request ? request.id : undefined;

    // Validate the dates.
    if (isNaN(start)) return callback({error: "invalid start", id: id}), -1;
    if (isNaN(stop)) return callback({error: "invalid stop", id: id}), -1;

    // Convert them to ObjectIDs.
    start = ObjectID.createFromTime(start/1000);
    stop = ObjectID.createFromTime(stop/1000);

    // Parse the expression.
    var expression;
    try {
      expression = parser.parse(request.expression);
    } catch (error) {
      return callback({error: "invalid expression", details:error, id: id}), -1;
    }

    // Set an optional limit on the number of events to return.
    var options = {sort: {_id: -1}, batchSize: 1000};
    if ("limit" in request) options.limit = +request.limit;

    // Copy any expression filters into the query object.
    var filter = {_id: {$gte: start, $lt: stop}};
    expression.filter(filter);

    // Request any needed fields.
    var fields = {_id:1};
    expression.fields(fields);

    // Query for the desired events.
    function query(callback) {
      collection(expression.type).events.find(filter, fields, options, function(error, cursor) {
        handle(error);
        cursor.each(function(error, event) {

          // If the callback is closed (i.e., if the WebSocket connection was
          // closed), then abort the query. Note that closing the cursor mid-
          // loop causes an error, which we subsequently ignore!
          if (callback.closed) return cursor.close();

          handle(error);
          // A null event indicates that there are no more results.
          if (event) callback({id: id, time: event._id.getTimestamp(), data: event.d});
          else callback(null);
        });
      });
    }

    // For streaming queries, share streams for efficient polling.
    if (stream) {
      var streams = streamsBySource[expression.source],
        initialResponseDone = false,
        anyData = false;

      // If there is an existing stream to attach to, backfill the initial set
      // of results to catch the client up to the stream. Add the new callback
      // to a queue, so that when the shared stream finishes its current poll,
      // it begins notifying the new client. Note that we don't pass the null
      // (end terminator) to the callback, because more results are to come!
      if (streams) {
        filter._id.$lt = streams.time;
        streams.waiting.push({
          id: id,
          callback: callback
        });
        query(function(event) {
          if (event) {
            anyData = true;
            callback(event);
          } else {
            // This is the end of the requested chunk, if we had previous data
            // there is no need to report to the client, otherwise let him know
            // we have nothing.
            if (!anyData) {
              callback({ id: id, time: filter._id.$lt.getTimestamp(), data: null });
            }
          }
        });
      }

      // Otherwise, we're creating a new stream, so we're responsible for
      // starting the polling loop. This means notifying active callbacks,
      // detecting when active callbacks are closed, advancing the time window,
      // and moving waiting clients to active clients.
      else {
        streams = streamsBySource[expression.source] = {time: stop, waiting: [], active: [{
          id: id,
          callback: callback
        }]};

        (function poll() {
          query(function(event) {

            // If there's an event, send it to all active, open clients.
            if (event) {
              anyData = true;
              streams.active.forEach(function(request) {
                if (!request.callback.closed) {
                  request.callback({
                    id: request.id,
                    time: event.time,
                    data: event.data
                  });
                }
              });
            }

            // Otherwise, we've reached the end of a poll, and it's time to
            // merge the waiting callbacks into the active callbacks. Advance
            // the time range, and set a timeout for the next poll.
            else {
              // On the 1st request only, tell the client whether we have data or not
              if (!initialResponseDone) {
                initialResponseDone = true;
                if (!anyData) {
                  callback({ id: id, time: filter._id.$lt.getTimestamp(), data: null });
                }
              }

              streams.active = streams.active.concat(streams.waiting).filter(open);
              streams.waiting = [];

              // If no clients remain, then it's safe to delete the shared
              // stream, and we'll no longer be responsible for polling.
              if (!streams.active.length) {
                delete streamsBySource[expression.source];
                return;
              }

              filter._id.$gte = streams.time;
              filter._id.$lt = streams.time = new ObjectID((Date.now() - delay)/1000);
              setTimeout(poll, streamInterval);
            }
          });
        })();
      }
    }

    // For non-streaming queries, just send the single batch!
    else query(callback);
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
