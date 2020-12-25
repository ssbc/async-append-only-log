var tape = require('tape')
var fs = require('fs')
var Log = require('../')

const filename = '/tmp/dsf-test-stream.log'

try { fs.unlinkSync(filename) } catch (_) {}
var log = Log(filename, {blockSize: 64*1024})

function B (fill, length) {
  var b = Buffer.alloc(length)
  b.fill(fill)
  return b
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

tape('empty', function (t) {
  log.stream({offsets: false}).pipe({
    paused: false,
    write: function () { throw new Error('should be empty') },
    end: t.end
  })
})

var v1 = B(0x10, 10)
tape('single', function (t) {
  log.append(v1, function (err) {
    t.notOk(err)
    log.onDrain(() => {
      log.stream({offsets: false}).pipe(collect(function (err, ary) {
        t.notOk(err)
        t.deepEqual(ary, [v1])
        t.end()
      }))
    })
  })
})

tape('single, reload', function (t) {
  log = Log(filename, {blockSize: 64*1024})
  log.stream({offsets: false}).pipe(collect(function (err, ary) {
    t.notOk(err)
    t.deepEqual(ary, [v1])
    t.end()
  }))
})
var v2 = B(0x20, 20)

tape('second', function (t) {
  log.append(v2, function (err) {
    t.notOk(err)
    log.onDrain(() => {
      log.stream({offsets: false}).pipe(collect(function (err, ary) {
        t.notOk(err)
        t.deepEqual(ary, [v1, v2])
        t.end()
      }))
    })
  })
})

var v3 = B(0x30, 30)
tape('live', function (t) {
  var sink = collect(function (err) {
    if (err === 'tape-ended') return
    else throw new Error('live stream should not end')
  })
  let ls = log.stream({live: true, offsets: false})
  ls.pipe(sink)
  log.append(v3, function (err) {})
  log.onDrain(function () {
    t.deepEqual(sink.array, [v1, v2, v3])
    sink.end('tape-ended')
    ls.abort()
    t.end()
  })
})

tape('offsets', function (t) {
  log.stream({offsets: true}).pipe(collect(function (err, ary) {
    t.notOk(err)
    t.deepEqual(ary, [{ offset: 0, value: v1}, { offset: 10 + 2, value: v2 }, { offset: 10 + 2 + 20 + 2, value: v3 }])
    t.end()
  }))
})

tape('limit', function (t) {
  log.stream({offsets: false, limit: 1}).pipe(collect(function (err, ary) {
    t.notOk(err)
    t.deepEqual(ary, [v1])
    t.end()
  }))
})

tape('limit gte', function (t) {
  log.stream({offsets: false, gte: 12, limit: 1}).pipe(collect(function (err, ary) {
    t.notOk(err)
    t.deepEqual(ary, [v2])
    t.end()
  }))
})

tape('gte', function (t) {
  log.stream({offsets: false, gte: 12}).pipe(collect(function (err, ary) {
    t.notOk(err)
    t.deepEqual(ary, [v2, v3])
    t.end()
  }))
})

tape('gt', function (t) {
  log.stream({offsets: false, gt: 12}).pipe(collect(function (err, ary) {
    t.notOk(err)
    t.deepEqual(ary, [v3])
    t.end()
  }))
})

tape('gt', function (t) {
  log.stream({offsets: false, gt: 0}).pipe(collect(function (err, ary) {
    t.notOk(err)
    t.deepEqual(ary, [v2, v3])
    t.end()
  }))
})

tape('gt -1', function (t) {
  log.stream({offsets: false, gt: -1}).pipe(collect(function (err, ary) {
    t.notOk(err)
    t.deepEqual(ary, [v1, v2, v3])
    t.end()
  }))
})

tape('live gt', function (t) {
  var v4 = B(0x40, 40)
  var sink = collect(function (err) {
    if (err === 'tape-ended') return
    else throw new Error('live stream should not end')
  })
  let ls = log.stream({ live: true, offsets: false, gt: 10 + 2 + 20 + 2 })
  ls.pipe(sink)
  log.append(v4, function (err) {})
  log.onDrain(() => {
    // need to wait for stream to get changes from save
    setTimeout(() => {
      t.deepEqual(sink.array, [v4])
      sink.end('tape-ended')
      ls.abort()
      t.end()
    }, 200)
  })
})

tape('double live', function (t) {
  const filename = '/tmp/dsf-test-stream-2.log'

  try { fs.unlinkSync(filename) } catch (_) {}
  var log = Log(filename, {blockSize: 64*1024})

  var i = 0

  log.stream({ live: true, offsets: false }).pipe({
    paused: false,
    write: function (data) {
      if (i == 0) {
        log.append(B(0x20, 20), function (err) {})
        ++i
      } else
        t.end()
    }
  })

  log.append(B(0x10, 10), function (err) {})
})

tape('close', function (t) {
  t.equal(log.streams.length, 0, 'no open streams')
  log.stream({offsets: false}).pipe({
    paused: false,
    write: function () {},
    end: function() {
      t.end()
    }
  })
  log.close(() => {})
})
