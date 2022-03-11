// SPDX-FileCopyrightText: 2022 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const tape = require('tape')
const push = require('push-stream')
const Log = require('../')

tape('delete first record, compact, stream', (t) => {
  const file = '/tmp/compaction-test_' + Date.now() + '.log'
  const log = Log(file, { blockSize: 10 })

  const buf1 = Buffer.from('first')
  const buf2 = Buffer.from('second')

  log.append(buf1, (err, offset1) => {
    log.append(buf2, (err, offset2) => {
      t.pass('append two records')
      log.onDrain(() => {
        log.del(offset1, (err) => {
          t.pass('delete first record')
          log.onDrain(() => {
            log.compact({}, (err) => {
              t.error(err, 'no error when compacting')
              log.onDrain(() => {
                log.stream({ offsets: false }).pipe(
                  push.collect((err, ary) => {
                    t.error(err, 'no error when streaming compacted log')
                    t.deepEqual(ary, [buf2], 'only second record exists')
                    log.close(t.end)
                  })
                )
              })
            })
          })
        })
      })
    })
  })
})

tape('compaction does not happen on the last (work in progress) block', (t) => {
  const file = '/tmp/compaction-test_' + Date.now() + '.log'
  const log = Log(file, { blockSize: 10 })

  const buf1 = Buffer.from('first')
  const buf2 = Buffer.from('second')

  log.append(buf1, (err, offset1) => {
    log.append(buf2, (err, offset2) => {
      t.pass('append two records')
      log.onDrain(() => {
        log.del(offset2, (err) => {
          t.pass('delete second record')
          log.onDrain(() => {
            log.compact({}, (err) => {
              t.error(err, 'no error when compacting')
              log.onDrain(() => {
                log.stream({ offsets: false }).pipe(
                  push.collect((err, ary) => {
                    t.error(err, 'no error when streaming compacted log')
                    t.deepEqual(ary, [buf1, null], 'not compacted last block')
                    log.close(t.end)
                  })
                )
              })
            })
          })
        })
      })
    })
  })
})

tape('shift many blocks', (t) => {
  const file = '/tmp/compaction-test_' + Date.now() + '.log'
  const log = Log(file, {
    blockSize: 11, // fits 3 records of size 3 plus EOB of size 2
    codec: {
      encode: (num) => Buffer.from(num.toString(16), 'hex'),
      decode: (buf) => parseInt(buf.toString('hex'), 16),
    },
  })

  log.append(
    [
      // block 0
      [0x11, 0x22, 0x33], // offsets: 0, 3, 6
      // block 1
      [0x44, 0x55, 0x66], // offsets: 11+0, 11+3, 11+6
      // block 2
      [0x77, 0x88, 0x99], // offsets: 22+0, 22+3, 22+6
      // block 3
      [0xaa, 0xbb, 0xcc], // offsets: 33+0, 33+3, 33+6
      // block 4
      [0xdd, 0xee, 0xff], // offsets: 44+0, 44+3, 44+6
    ].flat(),
    (err, offsets) => {
      t.pass('appended records')
      log.del(11 + 3, (err) => {
        log.del(11 + 6, (err) => {
          log.del(33 + 3, (err) => {
            t.pass('deleted some records in the middle')
            log.onDrain(() => {
              log.stream({ offsets: false }).pipe(
                push.collect((err, ary) => {
                  t.deepEqual(
                    ary,
                    [
                      // block 0
                      [0x11, 0x22, 0x33],
                      // block 1
                      [0x44, null, null],
                      // block 2
                      [0x77, 0x88, 0x99],
                      // block 3
                      [0xaa, null, 0xcc],
                      // block 4
                      [0xdd, 0xee, 0xff],
                    ].flat(),
                    'log has 4 blocks and some holes'
                  )
                  log.compact({}, (err) => {
                    t.error(err, 'no error when compacting')
                    log.onDrain(() => {
                      log.stream({ offsets: false }).pipe(
                        push.collect((err, ary) => {
                          t.error(err, 'no error when streaming compacted log')
                          t.deepEqual(
                            ary,
                            [
                              // block 0
                              [0x11, 0x22, 0x33],
                              // block 1
                              [0x44, 0x77, 0x88],
                              // block 2
                              [0x99, 0xaa, 0xcc],
                              // block 3
                              [0xdd, 0xee, 0xff],
                            ].flat(),
                            'log has 3 blocks and no holes'
                          )
                          log.close(t.end)
                        })
                      )
                    })
                  })
                })
              )
            })
          })
        })
      })
    }
  )
})
