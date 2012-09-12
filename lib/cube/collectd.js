exports.putter = function(putter) {
  var valuesByKey = {};
  var mappings = require("./configuration").get("collectd-plugin-mappings") || {};

  function defaultMapper(values) {
    var root = {host: values.host},
        data = root,
        parent,
        key;
    // The plugin and type are required. If the type is the same as the plugin,
    // then ignore the type (for example, memory/memory and load/load).
    parent = data, data = data[key = values.plugin] || (data[values.plugin] = {});
    if (values.type != values.plugin) parent = data, data = data[key = values.type] || (data[values.type] = {});

    // The plugin_instance and type_instance are optional: they are reported bycollected as an empty string 
    if (values.plugin_instance) root.plugin = values.plugin_instance;
    if (values.type_instance) root.type = values.type_instance;

    // If only a single value is specified, then don't store a map of named
    // values; just store the single value using the type_instance name (e.g.,
    // memory/memory-inactive, df-root/df_complex-used). Otherwise, iterate over
    // the received values and store them as a map.
    if (values.values.length == 1) parent[key] = value(0);
    else values.dsnames.forEach(function(d, i) { data[d] = value(i); });

    // For "derive" events, we must compute the delta since the last event.
    function value(i) {
      var d = values.values[i];
      switch (values.dstypes[i]) {
        case "derive": {
          var key =  values.host + "/" + values.plugin + "/" + values.plugin_instance + "/" + values.type + "/" + values.type_instance + "/" + values.dsnames[i],
              value = key in valuesByKey ? valuesByKey[key] : d;
          valuesByKey[key] = d;
          d -= value;
          break;
        }
      }
      return d;
    }

    if(root.type && root) {
      root[root.type] = root[values.plugin];
      delete root.type;
      delete root[values.plugin];
    }
    
    if(root.plugin) {
      //for is just an index, it's just a better name than "plugin"
      root.for =  root.plugin;
      delete root.plugin;
    }
    //remove the unnecessary things like disk.disk.disk_octets and move it to disk.octets
    if(typeof root[values.plugin] === "object") {
      
      Object.keys(root[values.plugin]).forEach(function(k){
        var originalkey = k;
        if(k.indexOf(values.plugin + "_") === 0 ) { //disk_octets -> octets
          k = k.slice(values.plugin.length+1)
        } 
        root[k] = root[values.plugin][originalkey];
      })
      delete root[values.plugin];
    }
    
    return {
      type: ""+values.plugin,
      time: new Date(+values.time),
      data: root
    }
  }

  // Converts a collectd value list to a Cube event.
  function event(values) {
    var typeMapper, newVal, oKey;

    //multiple plugins are likely to send the same kind of events
    //this code aims at enabling making from one plugin to another
    // e.g. : snmp and collectd reports roughly the same events,
    //this mapping ables to send snmp events in the same collections as collectd ones
    //so we can query them all at once with a simple query
    var pluginMapper = mappings[values.plugin];

    //FIXME : no remapping => not used, but may impact performances a bit
    function useRemapper(values, remapper) {
      var context = {
           /*
             this.emit sends the event back like it was a new event,
             this might lead to circular remapping,
             but it ables too to chain different remappings,
             which can be very useful.
           */
           emit: function emit(values) {
            emit.called = true;
            event(values);//synchronous way
            /*process.nextTick(event.bind(this, values)); //should we do this async ?*/
            return this;
          }
          ,"default": defaultMapper
        };

        context.emit.called = false;

        var val = remapper.call(context, values);
        if(context.emit.called) {
          return;
        }
        return val;
    }

    if(typeof pluginMapper === "function") {

      newVal = useRemapper(values, pluginMapper);
      return typeof newVal !== "undefined" ? newVal : values;

    } else if(typeof pluginMapper === "object") {

      typeMapper = pluginMapper[values.type];

      if(typeof typeMapper === "function") {
        
        newVal = useRemapper(values, typeMapper);
        return typeof newVal !== "undefined" ? newVal : values;
      } else if( typeof typeMapper === "string") {

        values.plugin = typeMapper || values.plugin;
        return defaultMapper(values);
      } else if( typeof typeMapper === "object") {
        for(oKey in typeMapper) { 
          values[oKey] = typeMapper[oKey];
        }
        return defaultMapper(values);
      } else if( typeof typeMapper === "undefined" && typeof pluginMapper["undefined"] === "function"){
        return pluginMapper["undefined"](values);
      }
      return defaultMapper(values);
    } else {
      return defaultMapper(values);
    }
  }

  return function(request, response) {
    var content = "";
    request.on("data", function(chunk) {
      content += chunk;
    });
    request.on("end", function() {
      var future = Date.now() / 1e3 + 1e9;
      JSON.parse(content).forEach(function(values) {
        var time = values.time;
        if (time > future) time /= 1073741824;
        values.time = Math.round(time) * 1e3;
        putter(event(values));
      });
      response.writeHead(200);
      response.end();
    });
  };
};
