var options = require("./evaluator-config"),
    cube = require("../"),
    server = cube.server(options);

server
  .use(cube.evaluator.register)
  .start()
