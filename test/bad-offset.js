// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

var tape = require('tape')
var fs = require('fs')
var Log = require('../')

tape('NaN', function (t) {
  var file = '/tmp/dsf-test-bad-offset.log'
  try {
    fs.unlinkSync(file)
  } catch (_) {}
  var db = Log(file, { blockSize: 2 * 1024 })

  var msg1 = Buffer.from('testing')

  db.append(msg1, function (err, offset1) {
    if (err) throw err
    t.equal(offset1, 0)
    db.get(NaN, function (err, b) {
      t.ok(err)
      t.match(err.message, /Offset NaN is not a number/, err.message)
      db.close(t.end)
    })
  })
})

tape('-1', function (t) {
  var file = '/tmp/dsf-test-bad-offset.log'
  try {
    fs.unlinkSync(file)
  } catch (_) {}
  var db = Log(file, { blockSize: 2 * 1024 })

  var msg2 = Buffer.from('testing')

  db.append(msg2, function (err, offset1) {
    if (err) throw err
    t.equal(offset1, 0)
    db.get(-1, function (err, b) {
      t.ok(err)
      t.match(err.message, /Offset -1 is negative/, err.message)
      db.close(t.end)
    })
  })
})
