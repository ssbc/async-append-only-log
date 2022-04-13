// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

var tape = require('tape')
var fs = require('fs')
var pify = require('util').promisify
var run = require('promisify-tuple')
var push = require('push-stream')
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

                db.onDeletesFlushed(() => {
                  db.get(offset3, function (err, deletedBuf) {
                    t.ok(err)
                    t.equal(err.message, 'Record has been deleted')
                    t.equal(err.code, 'ERR_AAOL_DELETED_RECORD')
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
        t.equal(err.message, 'Record has been deleted')
        t.equal(err.code, 'ERR_AAOL_DELETED_RECORD')

        db.del(offset2, function (err) {
          t.error(err)

          db.onDeletesFlushed(() => {
            db.get(offset2, function (err, deletedBuf) {
              t.ok(err)
              t.equal(err.message, 'Record has been deleted')
              t.equal(err.code, 'ERR_AAOL_DELETED_RECORD')
              // write changes
              db.close(t.end)
            })
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
      console.log(deletedBuf)
      t.ok(err)
      t.equal(err.message, 'Record has been deleted')
      t.equal(err.code, 'ERR_AAOL_DELETED_RECORD')

      db.close(t.end)
    })
  })
})

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
          db.onDeletesFlushed(() => {
            db.stream({ offsets: false }).pipe(
              push.collect((err, ary) => {
                t.notOk(err)
                t.deepEqual(ary, [null, buf2])
                db.close(t.end)
              })
            )
          })
        })
      })
    })
  })
})

tape('delete many', async (t) => {
  t.timeoutAfter(60e3)
  const file = '/tmp/aaol-test-delete-many' + Date.now() + '.log'
  const log = Log(file, { blockSize: 64 * 1024 })

  const TOTAL = 100000
  const offsets = []
  const logAppend = pify(log.append)
  console.time('append ' + TOTAL)
  for (let i = 0; i < TOTAL; i += 1) {
    const offset = await logAppend(Buffer.from(`hello ${i}`))
    offsets.push(offset)
  }
  t.pass('appended records')
  console.timeEnd('append ' + TOTAL)

  await pify(log.onDrain)()

  const logDel = pify(log.del)
  console.time('delete ' + TOTAL)
  for (let i = 0; i < TOTAL; i += 2) {
    await logDel(offsets[i])
  }
  console.timeEnd('delete ' + TOTAL)
  t.pass('deleted messages')

  await pify(log.onDeletesFlushed)()

  await new Promise((resolve) => {
    log.stream({ offsets: false }).pipe(
      push.collect((err, ary) => {
        t.error(err, 'no error on streaming')
        for (let i = 0; i < TOTAL; i += 1) {
          if (i % 2 === 0) {
            if (ary[i] !== null) t.fail('record '+ i + ' should be deleted')
          } else {
            if (ary[i] === null) t.fail('record '+ i + ' should be present')
          }
        }
        resolve()
      })
    )
  })

  await pify(log.close)()
  t.end()
})

tape('merge consecutive deletes', async function (t) {
  var file = '/tmp/test_del_merge_consecutives.log'
  try {
    fs.unlinkSync(file)
  } catch (_) {}
  var log = Log(file, { blockSize: 2 * 1024 })

  function b(str) {
    return Buffer.from(str.replace(/ +/g, ''), 'hex')
  }

  const [, offset1] = await run(log.append)(Buffer.from('abc')) //msg1)
  const [, offset2] = await run(log.append)(Buffer.from('def')) //msg2)
  const [, offset3] = await run(log.append)(Buffer.from('ghi')) //msg3)
  await run(log.onDrain)()
  const [errB1, blockBefore] = await run(log.getBlock)(0)
  t.error(errB1)
  const expectedBefore = b('0300 61 62 63 0300 64 65 66 0300 67 68 69')
  const actualBefore = blockBefore.slice(0, expectedBefore.length)
  console.log(expectedBefore)
  console.log(actualBefore)
  t.equals(actualBefore.compare(expectedBefore), 0, 'block buf looks okay')

  const [err1] = await run(log.del)(offset1)
  t.error(err1)
  const [err2] = await run(log.del)(offset2)
  t.error(err2)
  const [err3] = await run(log.del)(offset3)
  t.error(err3)
  await run(log.onDrain)()

  const [errB2, blockAfter] = await run(log.getBlock)(0)
  t.error(errB2)
  const expectedAfter = b('0d00 00 00 00 00 00 00 00 00 00 00 00 00 00')
  const actualAfter = blockAfter.slice(0, expectedAfter.length)
  console.log(expectedAfter)
  console.log(actualAfter)
  t.equals(actualAfter.compare(expectedAfter), 0, 'block buf looks okay')

  await run(log.close)()
  t.end()
})
