// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

var tape = require('tape')
var fs = require('fs')
var push = require('push-stream')
var Log = require('../')

const filename = '/tmp/dsf-test-stream.log'

try {
  fs.unlinkSync(filename)
} catch (_) {}
var log = Log(filename, { blockSize: 64 * 1024 })

function Buf(fill, length) {
  var b = Buffer.alloc(length)
  b.fill(fill)
  return b
}

tape('empty', function (t) {
  log.stream({ offsets: false }).pipe({
    paused: false,
    write: function () {
      throw new Error('should be empty')
    },
    end: t.end,
  })
})

var msg1 = Buf(0x10, 10)
tape('single', function (t) {
  log.append(msg1, function (err) {
    t.notOk(err)
    log.onDrain(() => {
      log.stream({ offsets: false }).pipe(
        push.collect((err, ary) => {
          t.notOk(err)
          t.deepEqual(ary, [msg1])
          t.end()
        })
      )
    })
  })
})

tape('single live pausable', function (t) {
  t.timeoutAfter(500)
  let i = 0
  let sink
  log.stream({ offsets: false, live: true }).pipe(
    (sink = {
      paused: false,
      write: function (buf) {
        t.deepEqual(buf, msg1)
        t.equal(i, 0)
        sink.paused = true
        setTimeout(() => {
          sink.paused = false
          sink.source.resume()
        })
        i++
      },
      end: function () {
        t.fail('should not end live stream')
      },
    })
  )
  setTimeout(t.end, 300)
})

tape('single, reload', function (t) {
  log = Log(filename, { blockSize: 64 * 1024 })
  log.stream({ offsets: false }).pipe(
    push.collect((err, ary) => {
      t.notOk(err)
      t.deepEqual(ary, [msg1])
      t.end()
    })
  )
})

var msg2 = Buf(0x20, 20)
tape('second', function (t) {
  log.append(msg2, function (err) {
    t.notOk(err)
    log.onDrain(() => {
      log.stream({ offsets: false }).pipe(
        push.collect((err, ary) => {
          t.notOk(err)
          t.deepEqual(ary, [msg1, msg2])
          t.end()
        })
      )
    })
  })
})

var msg3 = Buf(0x30, 30)
tape('live', function (t) {
  var sink = push.collect((err) => {
    if (err === 'tape-ended') return
    else throw new Error('live stream should not end')
  })
  let logStream = log.stream({ live: true, offsets: false })
  logStream.pipe(sink)
  log.append(msg3, function (err) {})
  log.onDrain(function () {
    t.deepEqual(sink.array, [msg1, msg2, msg3])
    sink.end('tape-ended')
    logStream.abort()
    t.end()
  })
})

tape('offsets', function (t) {
  log.stream({ offsets: true }).pipe(
    push.collect((err, ary) => {
      t.notOk(err)
      t.deepEqual(ary, [
        { offset: 0, value: msg1 },
        { offset: 10 + 2, value: msg2 },
        { offset: 10 + 2 + 20 + 2, value: msg3 },
      ])
      t.end()
    })
  )
})

tape('pausable', function (t) {
  let i = 0
  let sink
  log.stream({ offsets: false }).pipe(
    (sink = {
      paused: false,
      write: function (buf) {
        if (sink.paused) t.fail('should not write sink when it is paused')

        if (i === 0) {
          t.deepEqual(buf, msg1, 'msg1')
          sink.paused = true
          setTimeout(() => {
            sink.paused = false
            sink.source.resume()
          }, 100)
        }
        if (i === 1) {
          t.deepEqual(buf, msg2, 'msg2')
        }
        if (i === 2) {
          t.deepEqual(buf, msg3, 'msg3')
        }
        i++
      },
      end: function () {
        t.end()
      },
    })
  )
})

tape('limit', function (t) {
  log.stream({ offsets: false, limit: 1 }).pipe(
    push.collect((err, ary) => {
      t.notOk(err)
      t.deepEqual(ary, [msg1])
      t.end()
    })
  )
})

tape('limit gte', function (t) {
  log.stream({ offsets: false, gte: 12, limit: 1 }).pipe(
    push.collect((err, ary) => {
      t.notOk(err)
      t.deepEqual(ary, [msg2])
      t.end()
    })
  )
})

tape('gte', function (t) {
  log.stream({ offsets: false, gte: 12 }).pipe(
    push.collect((err, ary) => {
      t.notOk(err)
      t.deepEqual(ary, [msg2, msg3])
      t.end()
    })
  )
})

tape('gt', function (t) {
  log.stream({ offsets: false, gt: 12 }).pipe(
    push.collect((err, ary) => {
      t.notOk(err)
      t.deepEqual(ary, [msg3])
      t.end()
    })
  )
})

tape('gt 0', function (t) {
  log.stream({ offsets: false, gt: 0 }).pipe(
    push.collect((err, ary) => {
      t.notOk(err)
      t.deepEqual(ary, [msg2, msg3])
      t.end()
    })
  )
})

tape('gt -1', function (t) {
  log.stream({ offsets: false, gt: -1 }).pipe(
    push.collect((err, ary) => {
      t.notOk(err)
      t.deepEqual(ary, [msg1, msg2, msg3])
      t.end()
    })
  )
})

tape('live gt', function (t) {
  var msg4 = Buf(0x40, 40)
  var sink = push.collect((err) => {
    if (err === 'tape-ended') return
    else throw new Error('live stream should not end')
  })
  let logStream = log.stream({
    live: true,
    offsets: false,
    gt: 10 + 2 + 20 + 2,
  })
  logStream.pipe(sink)
  log.append(msg4, function (err) {})
  log.onDrain(() => {
    // need to wait for stream to get changes from save
    setTimeout(() => {
      t.deepEqual(sink.array, [msg4])
      sink.end('tape-ended')
      logStream.abort()
      t.end()
    }, 200)
  })
})

tape('live gt -1', function (t) {
  var msg5 = Buf(0x50, 50)
  var msg6 = Buf(0x50, 60)

  const filename1 = '/tmp/dsf-test-stream-1.log'
  try {
    fs.unlinkSync(filename1)
  } catch (_) {}
  var newLog = Log(filename1, { blockSize: 64 * 1024 })

  var sink = push.collect((err) => {
    if (err === 'tape-ended') return
    else throw new Error('live stream should not end')
  })
  let logStream = newLog.stream({ live: true, offsets: false, gt: -1 })
  logStream.pipe(sink)

  setTimeout(() => {
    sink.paused = true
    logStream.resume()
    sink.paused = false
    logStream.resume()
    newLog.append(msg5, function (err) {})
    newLog.append(msg6, function (err) {})
    newLog.onDrain(() => {
      // need to wait for stream to get changes from save
      setTimeout(() => {
        t.deepEqual(sink.array, [msg5, msg6])
        sink.end('tape-ended')
        logStream.abort()
        t.end()
      }, 200)
    })
  }, 100)
})

tape('double live', function (t) {
  const filename = '/tmp/dsf-test-stream-2.log'

  try {
    fs.unlinkSync(filename)
  } catch (_) {}
  var log = Log(filename, { blockSize: 64 * 1024 })

  var i = 0

  log.stream({ live: true, offsets: false }).pipe({
    paused: false,
    write: function (buf) {
      if (i === 0) {
        log.append(Buf(0x20, 20), function (err) {})
        ++i
      } else t.end()
    },
  })

  log.append(Buf(0x10, 10), function (err) {})
})

tape('close', function (t) {
  t.equal(log.streams.length, 0, 'no open streams')
  log.stream({ offsets: false }).pipe({
    paused: false,
    write: function () {},
    end: function () {
      t.end()
    },
  })
  log.close(() => {})
})
