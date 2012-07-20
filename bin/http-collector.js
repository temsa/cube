var options = require("./http-collector-config"),
    cube = require("../"),
    server = cube.server(options);

server
  .use(cube.collector.register)
  .start();
