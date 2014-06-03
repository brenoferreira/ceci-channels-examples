var core = require('ceci-core/es6');
var channels = require('ceci-channels');

var coreExample = function(){
  console.log("I am main");

  core.go(function*() {
    yield console.log("I am go block 1");
    yield console.log("I am go block 1");
  });

  core.go(function*() {
    yield console.log("I am go block 2");
    yield console.log("I am go block 2");
  });

  console.log("I am also main");
};

var channelsExample = function(){
  var ch = channels.chan();

  core.go(function*() {
    for (var i = 1; i <= 10; ++i)
      yield channels.push(ch, i);
    channels.close(ch);
  });

  core.go(function*() {
    var val;
    while (undefined !== (val = yield channels.pull(ch)))
      console.log(val);
  });
};

var mr = function(){
  var data = [
    ["jan", "walmart", 500],
    ["jan", "amazon", 400],
    ["feb", "walmart", 700],
    ["feb", "amazon", 500],
    ["mar", "walmart", 600],
    ["mar", "amazon", 500],
    ["apr", "walmart", 800],
    ["apr", "amazon", 100],
    ["may", "walmart", 600],
    ["may", "amazon", 300],
    ["jun", "walmart", 900],
    ["jun", "amazon", 200],
    ["jul", "walmart", 900],
    ["jul", "amazon", 300]
  ];

  var inputch = channels.chan();

  core.go(function*() {
    for (var i = 0; i < data.length; ++i)
      yield channels.push(inputch, data[i]);
    channels.close(inputch);
  });

  var mapch = channels.chan();
  //map phase
  core.go(function*() {
    var item;
    while (undefined !== (item = yield channels.pull(inputch))){
      var store = item[1];
      var sales = item[2];
      yield channels.push(mapch,[store, sales]);
    }

    channels.close(mapch);
  });

  //sort phase
  var sortch = channels.chan();

  core.go(function*() {
    var item;
    var data = [];
    while (undefined !== (item = yield channels.pull(mapch))) {
      data.push(item);
    }

    var sorted = data.sort(function(a, b) {
      if(a[0] > b[0]) return 1;
      if(a[0] < b[0]) return -1;
      return 0;
    });

    for (var i = 0; i < sorted.length; ++i)
      yield channels.push(sortch, sorted[i]);
    channels.close(sortch);
  });

  //reduce phase
  core.go(function*(){
    var item;
    var data = {};

    while (undefined !== (item = yield channels.pull(sortch))) {
      var store = item[0];
      var sales = item[1];
      if(data[store] === undefined){
        data[store] = 0;
      }

      data[store] += sales;
    }

    console.log(data);
  });
};

mr();