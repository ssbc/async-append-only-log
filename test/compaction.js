// SPDX-FileCopyrightText: 2022 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const tape = require('tape')
const push = require('push-stream')
const run = require('promisify-tuple')
const Log = require('../')

tape('delete first record, compact, stream', async (t) => {
  const file = '/tmp/compaction-test_' + Date.now() + '.log'
  const log = Log(file, { blockSize: 10 })

  const buf1 = Buffer.from('first')
  const buf2 = Buffer.from('second')

  const [, offset1] = await run(log.append)(buf1)
  const [, offset2] = await run(log.append)(buf2)
  await run(log.onDrain)()
  t.pass('append two records')

  await run(log.del)(offset1)
  await run(log.onDrain)()
  t.pass('delete first record')

  const [err] = await run(log.compact)({})
  await run(log.onDrain)()
  t.error(err, 'no error when compacting')

  await new Promise((resolve) => {
    log.stream({ offsets: false }).pipe(
      push.collect((err, ary) => {
        t.error(err, 'no error when streaming compacted log')
        t.deepEqual(ary, [buf2], 'only second record exists')
        resolve()
      })
    )
  })

  await run(log.close)()
  t.end()
})

tape('delete second record, compact, stream', async (t) => {
  const file = '/tmp/compaction-test_' + Date.now() + '.log'
  const log = Log(file, { blockSize: 10 })

  const buf1 = Buffer.from('first')
  const buf2 = Buffer.from('second')

  const [, offset1] = await run(log.append)(buf1)
  const [, offset2] = await run(log.append)(buf2)
  await run(log.onDrain)()
  t.pass('append two records')

  await run(log.del)(offset2)
  await run(log.onDrain)()
  t.pass('delete second record')

  await new Promise((resolve) => {
    log.stream({ offsets: false }).pipe(
      push.collect((err, ary) => {
        t.error(err, 'no error when streaming log')
        t.deepEqual(ary, [buf1, null], 'all blocks')
        resolve()
      })
    )
  })

  const [err] = await run(log.compact)({})
  await run(log.onDrain)()
  t.error(err, 'no error when compacting')

  await new Promise((resolve) => {
    log.stream({ offsets: false }).pipe(
      push.collect((err, ary) => {
        t.error(err, 'no error when streaming compacted log')
        t.deepEqual(ary, [buf1], 'last block truncated away')
        resolve()
      })
    )
  })

  await run(log.close)()
  t.end()
})

tape('shift many blocks', async (t) => {
  const file = '/tmp/compaction-test_' + Date.now() + '.log'
  const log = Log(file, {
    blockSize: 11, // fits 3 records of size 3 plus EOB of size 2
    codec: {
      encode: (num) => Buffer.from(num.toString(16), 'hex'),
      decode: (buf) => parseInt(buf.toString('hex'), 16),
    },
  })

  await run(log.append)(
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
    ].flat()
  )
  t.pass('appended records')

  await run(log.del)(11 + 3)
  await run(log.del)(11 + 6)
  await run(log.del)(33 + 3)
  await run(log.onDrain)()
  t.pass('deleted some records in the middle')

  await new Promise((resolve) => {
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
          'log has 5 blocks and some holes'
        )
        resolve()
      })
    )
  })

  const [err] = await run(log.compact)({})
  await run(log.onDrain)()
  t.error(err, 'no error when compacting')

  await new Promise((resolve) => {
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
          'log has 4 blocks and no holes, except in the last block'
        )
        resolve()
      })
    )
  })

  await run(log.close)()
  t.end()
})

tape('compact handling last deleted record on last block', async (t) => {
  const file = '/tmp/compaction-test_' + Date.now() + '.log'
  const log = Log(file, {
    blockSize: 11, // fits 3 records of size 3 plus EOB of size 2
    codec: {
      encode: (num) => Buffer.from(num.toString(16), 'hex'),
      decode: (buf) => parseInt(buf.toString('hex'), 16),
    },
  })

  await run(log.append)(
    [
      // block 0
      [0x11, 0x22, 0x33], // offsets: 0, 3, 6
      // block 1
      [0x44, 0x55, 0x66], // offsets: 11+0, 11+3, 11+6
      // block 2
      [0x77, 0x88, 0x99], // offsets: 22+0, 22+3, 22+6
    ].flat()
  )
  t.pass('appended records')

  await run(log.del)(11 + 3)
  await run(log.del)(22 + 6)
  await run(log.onDrain)()
  t.pass('deleted some records in the middle')

  await new Promise((resolve) => {
    log.stream({ offsets: false }).pipe(
      push.collect((err, ary) => {
        t.deepEqual(
          ary,
          [
            // block 0
            [0x11, 0x22, 0x33],
            // block 1
            [0x44, null, 0x66],
            // block 2
            [0x77, 0x88, null],
          ].flat(),
          'log has 3 blocks and some holes'
        )
        resolve()
      })
    )
  })

  const [err] = await run(log.compact)({})
  await run(log.onDrain)()
  t.error(err, 'no error when compacting')

  await new Promise((resolve) => {
    log.stream({ offsets: false }).pipe(
      push.collect((err, ary) => {
        t.error(err, 'no error when streaming compacted log')
        t.deepEqual(
          ary,
          [
            // block 0
            [0x11, 0x22, 0x33],
            // block 1
            [0x44, 0x66, 0x77],
            // block 2
            [0x88],
          ].flat(),
          'log has 3 blocks'
        )
        resolve()
      })
    )
  })

  await run(log.close)()
  t.end()
})

tape('compact handling holes of different sizes', async (t) => {
  const file = '/tmp/compaction-test_' + Date.now() + '.log'
  const log = Log(file, {
    blockSize: 14, // fits 4 records of size 3 plus EOB of size 2
    codec: {
      encode: (num) => Buffer.from(num.toString(16), 'hex'),
      decode: (buf) => parseInt(buf.toString('hex'), 16),
    },
  })

  await run(log.append)(
    [
      // block 0
      [0x11, 0x2222, 0x33], // offsets: 0, 3, 9
      // block 1
      [0x4444, 0x55, 0x66], // offsets: 14+0, 14+6, 14+9
      // block 2
      [0x77, 0x88, 0x99, 0xaa], // offsets: 28+0, 28+3, 28+6, 28+9
    ].flat()
  )
  t.pass('appended records')

  await run(log.del)(3)
  await run(log.del)(14 + 0)
  await run(log.onDrain)()
  t.pass('deleted some records in the middle')

  await new Promise((resolve) => {
    log.stream({ offsets: false }).pipe(
      push.collect((err, ary) => {
        t.deepEqual(
          ary,
          [
            // block 0
            [0x11, null, 0x33],
            // block 1
            [null, 0x55, 0x66],
            // block 2
            [0x77, 0x88, 0x99, 0xaa],
          ].flat(),
          'log has 3 blocks and some holes'
        )
        resolve()
      })
    )
  })

  const [err] = await run(log.compact)({})
  await run(log.onDrain)()
  t.error(err, 'no error when compacting')

  await new Promise((resolve) => {
    log.stream({ offsets: false }).pipe(
      push.collect((err, ary) => {
        t.error(err, 'no error when streaming compacted log')
        t.deepEqual(
          ary,
          [
            // block 0
            [0x11, 0x33, 0x55, 0x66],
            // block 1
            [0x77, 0x88, 0x99, 0xaa],
          ].flat(),
          'log has 2 blocks'
        )
        resolve()
      })
    )
  })

  await run(log.close)()
  t.end()
})
