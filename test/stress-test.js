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
    for (var i = 0; i < items; i++)
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

function collect (cb) {
  return {
    array: [],
    paused: false,
    write: function (v) { this.array.push(v) },
    end: function (err) {
      this.ended = err || true
      cb(err, this.array)
    }
  }
}

for (var run = 0; run < 10; ++run) {
  tape('live stress', function (t) {
    const filename = '/tmp/async-flumelog-live-stress.log'

    try { fs.unlinkSync(filename) } catch (_) {}
    var db = Log(filename, {
      blockSize: 64*1024,
      codec: require('flumecodec/json')
    })

    var sink = collect(function () {
      throw new Error('live stream should not end')
    })
    db.stream({live: true, seqs: false}).pipe(sink)

    var data = [], latestOffset = 0
    for (var i = 0; i < items; i++) {
      const d = {
        key: '#'+i,
        value: {
          foo: Math.random(), bar: Date.now()
        }
      }
      data.push(d)
      db.append(d, function (err, offset) {
        if (offset > latestOffset)
          latestOffset = offset
      })
    }

    function checkStreamDone() {
      if (!db.streams[0].writing) {
        t.deepEqual(sink.array.length, data.length)
        t.end()
      } else
        setTimeout(checkStreamDone, 200)
    }

    var remove = db.since(function (v) {
      if(v < latestOffset) return
      if (remove) remove()
      // this is crazy, db.since is set first, then streams are
      // resumed. So we need to wait for the stream to resume and
      // finish before we check thatwe got everything
      setTimeout(checkStreamDone, 200)
    })
  })
}
