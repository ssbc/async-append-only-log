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

var file = '/tmp/ds-test_restart.log'

var v1 = { v: 'hello world hello world' } 
var v2 = { v: 'hello world hello world 2' }

tape('simple', function (t) {
  try { fs.unlinkSync(file) } catch (_) {}
  var db = Offset(file, {
    block: 16*1024,
    codec: require('flumecodec/json')
  })
  
  db.append(v1, function (err, offset1) {
    if(err) throw err
    t.equal(offset1, 0)
    db.append(v2, function (err, offset2) {
      if(err) throw err
      t.equal(offset2, 33)

      db.onDrain(() => {
        db.stream({offsets: false}).pipe(collect(function (err, ary) {
          t.deepEqual(ary, [v1, v2])
          t.end()
        }))
      })
    })
  })
})

tape('simple reread', function (t) {
  var db = Offset(file, {
    block: 16*1024,
    codec: require('flumecodec/json')
  })

  db.onReady(() => {
    db.stream({offsets: false}).pipe(collect(function (err, ary) {
      t.deepEqual(ary, [v1, v2])
      t.end()
    }))
  })
})
