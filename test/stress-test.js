const tape = require('tape')
const fs = require('fs')
const Log = require('../')

const items = 20e3

for (var run = 0; run < 10; ++run) {
  tape('basic stress', function (t) {
    const filename = '/tmp/async-flumelog-basic-stress.log'
    
    try { fs.unlinkSync(filename) } catch (_) {}
    var db = Log(filename, {
      blockSize: 64*1024,
      codec: require('flumecodec/json')
    })
    
    var data = []
    for(var i = 0; i < items; i++)
      data.push({key: '#'+i, value: {
        foo: Math.random(), bar: Date.now()
      }})

    db.append(data, function (err, offset) {
      var remove = db.since(function (v) {
        if(v < offset) return
        remove()

        var result = []
        
        db.stream({seqs: false}).pipe({
          paused: false,
          write: function (e) { result.push(e) },
          end: function() {
            t.equal(result.length, data.length)
            //t.deepEqual(data, result)
            db.close(t.end)
          }
        })
      })
    })
  })
}

