// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

var tape = require('tape')
var fs = require('fs')
var Log = require('../')

const filename = '/tmp/dsf-idempotent-resume.log'

try {
  fs.unlinkSync(filename)
} catch (_) {}
var log = Log(filename, { blockSize: 64 * 1024 })

function Buf(fill, length) {
  var b = Buffer.alloc(length)
  b.fill(fill)
  return b
}

const TOTAL_RECORDS = 300_000
const getRecordLength = (i) => 1 + (i % 500)

tape('populate', function (t) {
  const records = Array(TOTAL_RECORDS)
    .fill(null)
    .map((x, i) => Buf(0x10, getRecordLength(i)))
  log.append(records, () => {
    log.onDrain(() => {
      t.end()
    })
  })
})

tape('a second resume() on the same stream is idempotent', function (t) {
  const stream = log.stream({ offsets: false })

  // The pipe causes the 1st resume to happen
  let i = 0
  stream.pipe({
    paused: false,
    offsets: false,
    write(buf) {
      const expected = getRecordLength(i)
      const actual = buf.length
      if (actual !== expected) {
        t.fail(`${i}-th record has ${actual} bytes, expected ${expected}`)
        process.exit(1) // otherwise the test will keep spamming many `t.fail`
      }
      i += 1
    },
    end() {
      t.equals(i, TOTAL_RECORDS)
      t.end()
    },
  })

  // This is the 2nd resume
  stream.resume()
})

tape('close', function (t) {
  t.equal(log.streams.length, 0, 'no open streams')
  log.close(() => {
    t.end()
  })
})
