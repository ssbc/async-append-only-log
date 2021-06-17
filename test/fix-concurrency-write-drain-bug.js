var tape = require('tape')
var fs = require('fs')
var Offset = require('../')

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

var file = '/tmp/ds-test_drain_since.log'

var v1 = { v: 'hello world hello world' } 

for (var i = 0; i < 1000; ++i) {
  tape('check since after drain', function (t) {
    try { fs.unlinkSync(file+i) } catch (_) {}
    var db = Offset(file + i, {
      block: 16*1024,
      writeTimeout: 1,
      codec: require('flumecodec/json')
    })

    db.onReady(() => {
      db.append(v1, function (err, offset1) {
        if(err) throw err

        setTimeout(() => {
          db.onDrain(() => {
            t.equal(db.since.value, 0, 'after drain offset is set')
            t.end()
          })
        }, 1)
      })
    })
  })
}
