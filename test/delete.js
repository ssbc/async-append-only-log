// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

var tape = require('tape')
var fs = require('fs')
var Log = require('../')

var msg1 = Buffer.from(
  'hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world'
)
var msg2 = Buffer.from(
  'hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db'
)
var msg3 = Buffer.from(
  'hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db'
)

tape('simple', function (t) {
  var file = '/tmp/fao-test_del.log'
  try {
    fs.unlinkSync(file)
  } catch (_) {}
  var db = Log(file, { blockSize: 2 * 1024 })

  db.append(msg1, function (err, offset1) {
    if (err) throw err
    t.equal(offset1, 0)
    db.append(msg2, function (err, offset2) {
      if (err) throw err
      db.append(msg3, function (err, offset3) {
        if (err) throw err
        t.ok(offset3 > offset2)
        db.get(offset1, function (err, buf) {
          if (err) throw err
          t.equal(buf.toString(), msg1.toString())

          db.get(offset2, function (err, buf) {
            if (err) throw err
            t.equal(buf.toString(), msg2.toString())

            db.get(offset3, function (err, buf) {
              if (err) throw err
              t.equal(buf.toString(), msg3.toString())

              db.del(offset3, function (err) {
                t.error(err)

                db.get(offset3, function (err, deletedBuf) {
                  t.ok(err)
                  t.equal(err.message, 'item has been deleted')
                  // write changes
                  db.onDrain(t.end)
                })
              })
            })
          })
        })
      })
    })
  })
})

tape('simple reread', function (t) {
  var file = '/tmp/fao-test_del.log'
  var db = Log(file, { blockSize: 2 * 1024 })

  var offset1 = 0
  var offset2 = msg1.length + 2
  var offset3 = msg1.length + 2 + msg2.length + 2

  db.get(offset1, function (err, buf) {
    if (err) throw err
    t.equal(buf.toString(), msg1.toString())

    db.get(offset2, function (err, buf) {
      if (err) throw err
      t.equal(buf.toString(), msg2.toString())

      db.get(offset3, function (err) {
        t.ok(err)
        t.equal(err.message, 'item has been deleted')

        db.del(offset2, function (err) {
          t.error(err)

          db.get(offset2, function (err, deletedBuf) {
            t.ok(err)
            t.equal(err.message, 'item has been deleted')
            // write changes
            db.close(t.end)
          })
        })
      })
    })
  })
})

tape('simple reread 2', function (t) {
  var file = '/tmp/fao-test_del.log'
  var db = Log(file, { blockSize: 2 * 1024 })

  db.get(0, function (err, buf) {
    if (err) throw err
    t.equal(buf.toString(), msg1.toString())

    db.get(msg1.length + 2, function (err, deletedBuf) {
      t.ok(err)
      t.equal(err.message, 'item has been deleted')

      t.end()
    })
  })
})

function collect(cb) {
  return {
    array: [],
    paused: false,
    write: function (v) {
      this.array.push(v)
    },
    end: function (err) {
      this.ended = err || true
      cb(err, this.array)
    },
  }
}

tape('stream delete', function (t) {
  var file = '/tmp/offset-test_' + Date.now() + '.log'
  var db = Log(file, { blockSize: 64 * 1024 })

  var buf2 = Buffer.from('hello offset db')

  db.append(Buffer.from('hello world'), function (err, offset1) {
    if (err) throw err
    db.append(buf2, function (err, offset2) {
      if (err) throw err
      db.del(offset1, function (err) {
        t.error(err)
        db.onDrain(() => {
          db.stream({ offsets: false }).pipe(
            collect(function (err, ary) {
              t.notOk(err)
              t.deepEqual(ary, [null, buf2])
              t.end()
            })
          )
        })
      })
    })
  })
})
