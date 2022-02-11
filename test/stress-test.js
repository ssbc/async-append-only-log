// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const tape = require('tape')
const fs = require('fs')
const Log = require('../')
const TooHot = require('too-hot')

const items = 10e3

function randomIntFromInterval(min, max) {
  return Math.floor(Math.random() * (max - min + 1) + min)
}

function randomStr(length) {
  let result = ''
  const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
  const charactersLength = characters.length;
  for (let i = 0; i < length; ++i)
    result += characters.charAt(Math.floor(Math.random() * 
                                           charactersLength))
  return result
}

for (var run = 0; run < 10; ++run) {
  tape('basic stress', function (t) {
    const filename = '/tmp/async-flumelog-basic-stress.log'
    const blockSize = randomIntFromInterval(12 * 1024, 64 * 1024)
    
    try { fs.unlinkSync(filename) } catch (_) {}
    var db = Log(filename, {
      blockSize,
      codec: require('flumecodec/json')
    })
    
    const originalStream = db.stream
    db.stream = function (opts) {
      const tooHot = TooHot({ceiling: 50, wait: 100, maxPause: Infinity})
      const s = originalStream(opts)
      const originalPipe = s.pipe.bind(s)
      s.pipe = function pipe(o) {
        let originalWrite = o.write
        o.write = (record) => {
          const hot = tooHot()
          if (hot && !s.sink.paused) {
            s.sink.paused = true
            hot.then(() => {
              originalWrite(record)
              s.sink.paused = false
              s.resume()
            })
          } else {
            originalWrite(record)
          }
        }
        return originalPipe(o)
      }
      return s
    }

    var data = []
    for (var i = 0; i < items; i++) {
      o = {key: '#'+i, value: {
        s: randomStr(randomIntFromInterval(100, 8000)),
        foo: Math.random(), bar: Date.now()
      }}
      if (i % 10 === 0)
        o.value.baz = randomIntFromInterval(1, 1500)
      if (i % 3 === 0)
        o.value.cat = randomIntFromInterval(1, 1500)
      if (i % 2 === 0)
        o.value.hat = randomIntFromInterval(1, 1500)
      data.push(o)
    }

    db.append(data, function (err, offset) {
      var remove = db.since(function (v) {
        if(v < offset) return
        remove()

        var result = []
        var stream1Done = false, stream2Done = false
        
        db.stream({offsets: false}).pipe({
          paused: false,
          write: function (e) { result.push(e) },
          end: function() {
            t.equal(result.length, data.length)
            //t.deepEqual(data, result)
            if (stream2Done)
              db.close(t.end)
            else
              stream1Done = true
          }
        })

        var result2 = []

        db.stream({offsets: false}).pipe({
          paused: false,
          write: function (e) { result2.push(e) },
          end: function() {
            t.equal(result2.length, data.length)
            //t.deepEqual(data, result)
            if (stream1Done)
              db.close(t.end)
            else
              stream2Done = true
          }
        })
      })
    })
  })
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

for (var run = 0; run < 10; ++run) {
  tape('live stress', function (t) {
    const filename = '/tmp/async-flumelog-live-stress.log'

    try { fs.unlinkSync(filename) } catch (_) {}
    var db = Log(filename, {
      blockSize: 64*1024,
      writeTimeout: 10,
      codec: require('flumecodec/json')
    })

    const originalStream = db.stream
    db.stream = function (opts) {
      const tooHot = TooHot({ceiling: 90, wait: 100, maxPause: Infinity})
      const s = originalStream(opts)
      const originalPipe = s.pipe.bind(s)
      s.pipe = function pipe(o) {
        let originalWrite = o.write.bind(o)
        o.write = (record) => {
          const hot = tooHot()
          if (hot && !s.sink.paused) {
            //console.log("Hot in here", hot)
            s.sink.paused = true
            hot.then(() => {
              originalWrite(record)
              s.sink.paused = false
              s.resume()
            })
          } else {
            originalWrite(record)
          }
        }
        return originalPipe(o)
      }
      return s
    }

    var sink = collect(function () {
      throw new Error('live stream should not end')
    })
    db.stream({live: true, offsets: false}).pipe(sink)

    var data = [], latestOffset = 0
    for (var i = 0; i < items; i++) {
      const d = {
        key: '#'+i,
        value: {
          foo: Math.random(), bar: Date.now()
        }
      }
      data.push(d)
      db.append(d, function (err, offset) {
        if (offset > latestOffset)
          latestOffset = offset
      })
    }

    function checkStreamDone() {
      if (sink.array.length === data.length) {
        t.deepEqual(sink.array, data)
        t.end()
      } else
        setTimeout(checkStreamDone, 200)
    }

    var remove = db.since(function (v) {
      if(v < latestOffset) return
      if (remove) remove()
      // this is crazy, db.since is set first, then streams are
      // resumed. So we need to wait for the stream to resume and
      // finish before we can check that we got everything
      setTimeout(checkStreamDone, 200)
    })
  })
}

for (var run = 0; run < 10; ++run) {
  tape('resume stress', function (t) {
    const filename = '/tmp/async-flumelog-live-stress.log'

    try { fs.unlinkSync(filename) } catch (_) {}
    var db = Log(filename, {
      blockSize: 64*1024,
      writeTimeout: 10,
      codec: require('flumecodec/json')
    })

    const originalStream = db.stream
    db.stream = function (opts) {
      const tooHot = TooHot({ceiling: 90, wait: 100, maxPause: Infinity})
      const s = originalStream(opts)
      const originalPipe = s.pipe.bind(s)
      s.pipe = function pipe(o) {
        let originalWrite = o.write.bind(o)
        o.write = (record) => {
          const hot = tooHot()
          if (hot && !s.sink.paused) {
            //console.log("Hot in here", hot)
            s.sink.paused = true
            hot.then(() => {
              originalWrite(record)
              s.sink.paused = false
              s.resume()
            })
          } else {
            originalWrite(record)
          }
        }
        return originalPipe(o)
      }
      return s
    }

    var sink = collect(function () {
      throw new Error('live stream should not end')
    })
    const stream = db.stream({live: true, offsets: false})
    stream.pipe(sink)

    var data = [], latestOffset = 0
    for (var i = 0; i < items; i++) {
      const d = {
        key: '#'+i,
        value: {
          foo: Math.random(), bar: Date.now()
        }
      }
      data.push(d)
      db.append(d, function (err, offset) {
        if (offset > latestOffset)
          latestOffset = offset
      })
    }

    function checkStreamDone() {
      stream.resume() // stress test this

      if (sink.array.length === data.length) {
        t.deepEqual(sink.array, data)
        t.end()
      } else
        setTimeout(checkStreamDone, randomIntFromInterval(50,200))
    }

    var remove = db.since(function (v) {
      if(v < latestOffset) return
      if (remove) remove()
      // this is crazy, db.since is set first, then streams are
      // resumed. So we need to wait for the stream to resume and
      // finish before we can check that we got everything
      setTimeout(checkStreamDone, 200)
    })
  })
}
