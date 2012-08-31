exports.putter = function(putter) {
  var valuesByKey = {};

  // Converts a collectd value list to a Cube event.
  function event(values) {
    require('colors')
var util = require('util')
function inspect (inspected) {return util.inspect(inspected,true,5,true)}
function log(key,value) {console.log(('\n'+key+':').bold.red, inspect(value),'\n')}
function proxy(fn) {return function proxyfier(){log('proxy args',arguments); var res=fn.apply(this,arguments); log('proxy res', res); return res}}

    var root = {host: values.host},
        data = root,
        parent,
        key;

    //as snmp and collectd reports roughly the same events,
    //this mapping ables to drop snmp events in the same collections as collectd ones
    if(values.plugin === 'snmp') {
      var typeEquiv = require("./configuration").get("collectd-snmp-plugin-equivalences")[values.type];
      if(typeof typeEquiv === "function") {
        var newVal = typeEquiv(values)
        values = typeof newval !== "undefined" ? newVal : values;
      } else if( typeof typeEquiv === "string") {
        values.plugin = typeEquiv || values.plugin;
      } else if( typeof typeEquiv === "object") {
        for(var oKey in typeEquiv) { 
          values[oKey] = typeEquiv[oKey];
        }
      }
    }
    
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

  return function(request, response) {
    var content = "";
    request.on("data", function(chunk) {
      content += chunk;
    });
    request.on("end", function() {
console.log('content',content)
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
