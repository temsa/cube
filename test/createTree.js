var vows = require("vows"),
    assert = require("assert"),
    createTree = require("../lib/cube/inlineName").createTree

var suite = vows.describe("collector");

suite.addBatch({
  "a simple object": {
    topic: createTree({ type:"test"}),
    "has the expected event type, test": function(e) {
      assert.equal(e.type, "test");
    }
  }
});


suite.addBatch({
  "an object with one point": {
    topic: createTree((function(){ var r = {}; r["test.ind"]=16; return r;})()),
    "has the expected event type, test": function(e) {
      assert.ok(e, "ok");
      assert.ok(e.test, "");
      assert.ok(e.test.ind,"no ind property");
      assert.equal(e.test.ind,16);
    }
  }
});

suite.addBatch({
  "an object with two points": {
    topic: createTree((function(){ var r = {}; r["test.ind.ien"]=52; return r;})()),
    "has the expected event type, test": function(e) {
      assert.ok(e, "ok");
      assert.ok(e.test, "");
      assert.ok(e.test.ind,"no ind property");
      assert.ok(e.test.ind.ien,"no ind property");
      assert.equal(e.test.ind.ien,52);
    }
  }
});

suite.addBatch({
  "an object with 2 object with one points": {
    topic: createTree((function(){ var r = {}; r["test.un"]=52; r["test2.dex"]= "chien"; return r;})()),
    "has the expected event type, test": function(e) {
      assert.ok(e, "ok");
      assert.ok(e.test, "");
      assert.ok(e.test.un,"no un property");

      assert.ok(e.test2, "");
      assert.ok(e.test2.dex,"no dex property");
      assert.equal(e.test2.dex,"chien");
    }
  }
});

suite.addBatch({
  "an object with 1 object with 2 sub objects one points": {
    topic: createTree((function(){ var r = {}; r["test.un"]=52; r["test.dex"]= "chien"; return r;})()),
    "has the expected event type, test": function(e) {
      assert.ok(e, "ok");
      assert.ok(e.test, "");
      assert.ok(e.test.un,"no un property");

      assert.ok(e.test, "");
      assert.ok(e.test.dex,"no dex property");
      assert.equal(e.test.dex,"chien");

    }
  }
});


suite.addBatch({
  "an complex object": {
    topic: createTree((function(){ var r = {}; 
        r["test.un"]=52; 
        r["test.dex"]= "chien"; 
        r["test3.kkj.io.l"] =  "kiil";
        r["test4.kkj.io.laa"] =  "oups";
        r["jj"]=25;
        r["kkk.lm"]={lkkl:"mml"}

        return r;})()),
    "has the expected event type, test": function(e) {
      assert.ok(e, "ok");
       assert.ok(e.test, "ok");
      assert.ok(e.test.un, "ok");
      assert.ok(e.test.dex, "ok");
      assert.ok(e.test3, "ok");
      assert.ok(e.test3.kkj, "ok");
      assert.ok(e.test3.kkj.io, "ok");
      assert.ok(e.test3.kkj.io.l, "ok");
      assert.equal(e.test3.kkj.io.l, "kiil");

      assert.ok(e.test4, "ok");
      assert.ok(e.test4.kkj, "ok");
      assert.ok(e.test4.kkj.io, "ok");
      assert.ok(e.test4.kkj.io.laa, "ok");
      assert.equal(e.test4.kkj.io.laa, "oups");

      assert.ok(e.jj, "ok");
      assert.equal(e.jj, 25);

      assert.ok(e.kkk, "ok");
      assert.ok(e.kkk.lm, "ok");
      assert.ok(e.kkk.lm.lkkl, "ok");
      assert.equal(e.kkk.lm.lkkl, "mml");

    }
  }
});


suite.addBatch({
  "an object with 1 object with 2 points following": {
    topic: createTree((function(){ var r = {}; r["test..un"]=52; return r;})()),
    "has the expected event type, test": function(e) {
      assert.ok(e, "ok");
      assert.ok(e.test, "");
      assert.ok(e.test[''],"no '' property");
      assert.ok(e.test[''].un,"no '' property");
      assert.equal(e.test[''].un,52);
    }
  }
});

suite.export(module);
