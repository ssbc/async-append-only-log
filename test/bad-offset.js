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
      t.equal(err, 'Offset NaN is not a number')
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
      t.equal(err, 'Offset is -1 must be >= 0')
      db.close(t.end)
    })
  })
})
