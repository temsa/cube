var configuration = module.exports = {
  get: function getConfiguration(type, localDefault){
    return configuration.options[type] || configuration.defaults[type] || localDefault 
  },
  options:{}, // will be updated by the server during the start
  defaults:{
    "events-db-metric": {
      capped: true,
      size: 1e7,
      autoIndexId: true
    },
    "events-db-event-indexes": [{key: {"d.id": 1}, params:{unique: true, sparse: true}}],
    "events-db-metric-indexes": [
      {key: {"i": 1, "_id.e": 1, "_id.l": 1, "_id.t": 1}},
      {key: {"i": 1, "_id.l": 1, "_id.t": 1}}
    ],
    "collectd-plugin-mappings": {
      "snmp":{
        "if_octets":"interface",
        "disk_octets":"disk",
        "swap_io":"swap",
        "swap":"swap"
      }
    }
  }
}
