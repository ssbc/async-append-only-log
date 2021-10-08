// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

var tape = require('tape')
var fs = require('fs')
var Log = require('../')

var v1 = Buffer.from('hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world hello world')
var v2 = Buffer.from('hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db hello offset db')
var v3 = Buffer.from('hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db hello offsetty db')

tape('simple', function (t) {
  var file = '/tmp/fao-test_del.log'
  try { fs.unlinkSync(file) } catch (_) {}
  var db = Log(file, {blockSize: 2*1024})

  db.append(v1, function (err, offset1) {
    if(err) throw err
    t.equal(offset1, 0)
    db.append(v2, function (err, offset2) {
      if(err) throw err
      db.append(v3, function (err, offset3) {
        if(err) throw err
        t.ok(offset3 > offset2)
        db.get(offset1, function (err, b) {
          if(err) throw err
          t.equal(b.toString(), v1.toString())

          db.get(offset2, function (err, b2) {
            if(err) throw err
            t.equal(b2.toString(), v2.toString())

            db.get(offset3, function (err, b3) {
              if(err) throw err
              t.equal(b3.toString(), v3.toString())

              db.del(offset3, function (err) {
                t.error(err)

                db.get(offset3, function (err, bdel) {
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
  var db = Log(file, {blockSize: 2*1024})

  var offset1 = 0
  var offset2 = v1.length+2
  var offset3 = v1.length+2 + v2.length+2

  db.get(offset1, function (err, b) {
    if(err) throw err
    t.equal(b.toString(), v1.toString())

    db.get(offset2, function (err, b2) {
      if(err) throw err
      t.equal(b2.toString(), v2.toString())

      db.get(offset3, function (err) {
        t.ok(err)
        t.equal(err.message, 'item has been deleted')

        db.del(offset2, function (err) {
          t.error(err)

          db.get(offset2, function (err, bdel) {
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
  var db = Log(file, {blockSize: 2*1024})

  db.get(0, function (err, b) {
    if(err) throw err
    t.equal(b.toString(), v1.toString())

    db.get(v1.length+2, function (err, b2) {
      t.ok(err)
      t.equal(err.message, 'item has been deleted')

      t.end()
    })
  })
})

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

tape('stream delete', function(t) {
  var file = '/tmp/offset-test_'+Date.now()+'.log'
  var db = Log(file, {blockSize: 64*1024})

  var b2 = Buffer.from('hello offset db')
  
  db.append(Buffer.from('hello world'), function (err, offset1) {
    if(err) throw err
    db.append(b2, function (err, offset2) {
      if(err) throw err
      db.del(offset1, function (err) {
        t.error(err)
        db.onDrain(() => {
          db.stream({offsets: false}).pipe(collect(function (err, ary) {
            t.notOk(err)
            t.deepEqual(ary, [null, b2])
            t.end()
          }))
        })
      })
    })
  })
})
