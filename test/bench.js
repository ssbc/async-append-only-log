// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const os = require('os')
const fs = require('fs')
const path = require('path')

const pull = require('pull-stream')
const cont = require('cont')
const Flume = require('flumedb')
const Index = require('flumeview-level')

const Log = require('../')
const toCompat = require('../compat')

console.log('name, ops, opts/second, seconds')
function Timer(name) {
  var start = Date.now()
  return function (ops) {
    var seconds = (Date.now() - start) / 1000
    console.log([name, ops, ops / seconds, seconds].join(', '))
  }
}

function initialize(db, N, _, cb) {
  var data = []
  for (var i = 0; i < N; i++)
    data.push({
      key: '#' + i,
      value: {
        foo: Math.random(),
        bar: Date.now(),
      },
    })

  db.append(data, function (err, offset) {
    if (err) throw err
    //wait until the view is consistent!
    var remove = db.index.since(function (v) {
      if (v < offset) return
      remove()
      cb(null, N)
    })
  })
}

function ordered_para(db, N, _, cb) {
  //ordered reads
  var n = 0
  for (var i = 0; i < N; i++) {
    db.index.get('#' + i, next)
  }

  function next(err, v) {
    if (err) return cb(err)
    if (++n === N) cb(null, N)
  }
}

function ordered_series(db, N, _, cb) {
  //ordered reads
  var n = 0,
    i = 0
  ;(function _next() {
    var key = '#' + i++
    db.index.get(i, function (err, msg) {
      if (msg.key !== key) return cb('benchmark failed: incorrect key returned')
      if (i === n) cb(null, N)
      else setImmediate(_next)
    })
  })(0)
}

function random_series(db, N, _, cb) {
  ;(function get(i) {
    if (i >= N) return cb(null, N)

    db.index.get('#' + ~~(Math.random() * N), function (err, value) {
      if (err) return cb(err)
      setImmediate(function () {
        get(i + 1)
      })
    })
  })(0)
}

function random_para(db, N, _, cb) {
  var n = 0
  for (var i = 0; i < N; i++) db.index.get('#' + ~~(Math.random() * N), next)

  function next(err, value) {
    if (err && n >= 0) {
      n = -1
      cb(err)
    } else if (++n === N) cb(null, N)
  }
}

function random_ranges(db, N, makeOpts, cb) {
  if (!db.index.read) return cb(new Error('not supported'))
  ;(function get(i) {
    if (i >= N) return cb(null, N)

    pull(
      db.index.read(makeOpts('#' + ~~(Math.random() * N))),
      pull.collect(function (err, ary) {
        if (err) return cb(err)
        setImmediate(function () {
          get(i + ary.length)
        })
      })
    )
  })(0)
}

function limit10(key) {
  return { gt: key, limit: 10, keys: false }
}

function create(dir, seed) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir)

  var raf = Log(dir + '/aligned.log', {
    blockSize: 1024 * 64,
    codec: require('flumecodec/json'),
  })

  return Flume(toCompat(raf)).use(
    'index',
    Index(1, function (msg) {
      return [msg.key]
    })
  )
}

var seed = Date.now()
var dir = path.join(os.tmpdir(), `test-async-flumelog-bench-index-${seed}`)
var db = create(dir, seed)
var N = 50e3

function refresh() {
  return function (cb) {
    db.close(function () {
      db = create(dir, seed)
      var start = Date.now()
      var rm = db.index.since(function (msg) {
        if (msg === db.since.value) {
          console.error('reload', Date.now() - start)
          rm()
          cb()
        }
      })
    })
  }
}

function run(name, benchmark, opts) {
  return function (cb) {
    var t = Timer(name)
    benchmark(db, N, opts, function (err, n) {
      t(err || n)
      cb()
    })
  }
}

cont.series(
  [
    run('append', initialize),
    run('ordered_para', ordered_para),
    run('random_para', random_para),
    run('ordered_series', ordered_para),
    run('random_series', random_para),
    refresh(),
    run('ordered_para (cool)', ordered_para),
    run('ordered_para (warm)', ordered_para),
    refresh(),
    run('random_para (cool)', random_para),
    run('random_para (warm)', random_para),
    refresh(),
    run('ordered_series (cool)', ordered_para),
    run('ordered_series (warm)', ordered_para),
    refresh(),
    run('random_series (cool)', random_para),
    run('random_series (warm)', random_para),
    refresh(),
    run('random-ranges', random_ranges, limit10),
  ].filter(Boolean)
)(function () {
  db.close()
})
