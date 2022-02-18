// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

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

var msg1 = { text: 'hello world hello world' }
var msg2 = { text: 'hello world hello world 2' }

tape('simple', function (t) {
  try { fs.unlinkSync(file) } catch (_) {}
  var db = Offset(file, {
    block: 16*1024,
    codec: require('flumecodec/json')
  })

  db.append(msg1, function (err, offset1) {
    if(err) throw err
    t.equal(offset1, 0)
    db.append(msg2, function (err, offset2) {
      if(err) throw err
      t.equal(offset2, 36)

      db.onDrain(() => {
        db.stream({offsets: false}).pipe(collect(function (err, ary) {
          t.deepEqual(ary, [msg1, msg2])
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
      t.deepEqual(ary, [msg1, msg2])
      t.end()
    }))
  })
})
