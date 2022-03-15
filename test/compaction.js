// SPDX-FileCopyrightText: 2022 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const tape = require('tape')
const fs = require('fs')
const push = require('push-stream')
const run = require('promisify-tuple')
const timer = require('util').promisify(setTimeout)
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

tape('recovers from crash just after persisting state', async (t) => {
  t.timeoutAfter(6000)
  const file = '/tmp/compaction-test_' + Date.now() + '.log'
  let log = Log(file, {
    blockSize: 9,
    codec: {
      encode: (num) => Buffer.from(num.toString(16), 'hex'),
      decode: (buf) => parseInt(buf.toString('hex'), 16),
    },
  })
  t.pass('suppose the log has blockSize 9')
  t.pass('suppose we had blocks: [null, 0x22] and [0x33, 0x44]')

  await run(log.append)(
    [
      // block 0
      [0x22, 0x33], // offsets: 0, 3
      // block 1
      [0x33, 0x44], // offsets: 9+0, 9+3
    ].flat()
  )
  await run(log.close)()
  t.pass('suppose compaction was in progress: [0x22, 0x33] and [0x33, 0x44]')

  const compactingBlockIndex = [1, 0] // uint16LE
  const unshiftedOffset = [9 + 3 + 1, 0] // uint32LE
  const unshiftedBlock = [
    [1, 0, 0x33],
    [1, 0, 0x44],
    [0, 0, 0],
  ].flat()
  await fs.promises.writeFile(
    file + '.compaction',
    Buffer.from([
      ...compactingBlockIndex,
      ...unshiftedOffset,
      ...unshiftedBlock,
    ])
  )
  t.pass('suppose compaction file: blockIndex 1, unshifted 12, [0x33, 0x44]')

  log = Log(file, {
    blockSize: 9,
    codec: {
      encode: (num) => Buffer.from(num.toString(16), 'hex'),
      decode: (buf) => parseInt(buf.toString('hex'), 16),
    },
  })
  t.pass('start log, compaction should autostart')

  await timer(1000)

  await new Promise((resolve) => {
    log.stream({ offsets: false }).pipe(
      push.collect((err, ary) => {
        t.error(err, 'no error when streaming compacted log')
        t.deepEqual(
          ary,
          [
            // block 0
            [0x22, 0x33],
            // block 1
            [0x44],
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

tape('recovers from crash just after persisting block', async (t) => {
  t.timeoutAfter(6000)
  const file = '/tmp/compaction-test_' + Date.now() + '.log'
  let log = Log(file, {
    blockSize: 9,
    codec: {
      encode: (num) => Buffer.from(num.toString(16), 'hex'),
      decode: (buf) => parseInt(buf.toString('hex'), 16),
    },
  })
  t.pass('suppose the log has blockSize 9')
  t.pass('suppose we had blocks: [null, 0x22] and [0x33, 0x44]')

  await run(log.append)(
    [
      // block 0
      [0x22, 0x33], // offsets: 0, 3
      // block 1
      [0x33, 0x44], // offsets: 9+0, 9+3
    ].flat()
  )
  await run(log.close)()
  t.pass('suppose compaction was in progress: [0x22, 0x33] and [0x33, 0x44]')

  const compactingBlockIndex = [0, 0] // uint16LE
  const unshiftedOffset = [0 + 1, 0] // uint32LE
  const unshiftedBlock = [
    [2, 0, 0, 0], // deleted. used to be [2, 0, 0x11, 0x11]
    [1, 0, 0x22],
    [0, 0],
  ].flat()
  await fs.promises.writeFile(
    file + '.compaction',
    Buffer.from([
      ...compactingBlockIndex,
      ...unshiftedOffset,
      ...unshiftedBlock,
    ])
  )
  t.pass('suppose compaction file: blockIndex 0, unshifted 0, [null, 0x22]')

  log = Log(file, {
    blockSize: 9,
    codec: {
      encode: (num) => Buffer.from(num.toString(16), 'hex'),
      decode: (buf) => parseInt(buf.toString('hex'), 16),
    },
  })
  t.pass('start log, compaction should autostart')

  await timer(1000)

  await new Promise((resolve) => {
    log.stream({ offsets: false }).pipe(
      push.collect((err, ary) => {
        t.error(err, 'no error when streaming compacted log')
        t.deepEqual(
          ary,
          [
            // block 0
            [0x22, 0x33],
            // block 1
            [0x44],
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

tape('append during compaction is postponed', async (t) => {
  const file = '/tmp/compaction-test_' + Date.now() + '.log'
  const log = Log(file, { blockSize: 10 })

  const buf1 = Buffer.from('first')
  const buf2 = Buffer.from('second')
  const buf3 = Buffer.from('third')

  const [, offset1] = await run(log.append)(buf1)
  const [, offset2] = await run(log.append)(buf2)
  await run(log.onDrain)()
  t.pass('append two records')

  await run(log.del)(offset1)
  await run(log.onDrain)()
  t.pass('delete first record')

  let appendDone = false
  let compactDone = false
  log.compact({}, (err) => {
    t.error(err, 'no error when compacting')
    t.false(appendDone, 'compact was done before append')
    compactDone = true
  })
  const [err, offset3] = await run(log.append)(buf3)
  appendDone = true
  t.error(err, 'no error when appending')
  t.equal(offset3, 10, 'append wrote "third" on the 2nd block')
  t.true(compactDone, 'compaction was done by the time append is done')
  await run(log.onDrain)()

  await new Promise((resolve) => {
    log.stream({ offsets: false }).pipe(
      push.collect((err, ary) => {
        t.error(err, 'no error when streaming compacted log')
        t.deepEqual(ary, [buf2, buf3], 'only 2nd and 3rd records exist')
        resolve()
      })
    )
  })

  await run(log.close)()
  t.end()
})

tape('appendTransaction during compaction is postponed', async (t) => {
  const file = '/tmp/compaction-test_' + Date.now() + '.log'
  const log = Log(file, { blockSize: 10 })

  const buf1 = Buffer.from('first')
  const buf2 = Buffer.from('second')
  const buf3 = Buffer.from('third')

  const [, offset1] = await run(log.append)(buf1)
  const [, offset2] = await run(log.append)(buf2)
  await run(log.onDrain)()
  t.pass('append two records')

  await run(log.del)(offset1)
  await run(log.onDrain)()
  t.pass('delete first record')

  let appendTransactionDone = false
  let compactDone = false
  log.compact({}, (err) => {
    t.error(err, 'no error when compacting')
    t.false(appendTransactionDone, 'compact was done before appendTransaction')
    compactDone = true
  })
  const [err, offset3] = await run(log.appendTransaction)([buf3])
  appendTransactionDone = true
  t.error(err, 'no error when appending')
  t.deepEquals(offset3, [10], 'appendTransaction wrote "third" on 2nd block')
  t.true(compactDone, 'compaction was done before appendTransaction done')
  await run(log.onDrain)()

  await new Promise((resolve) => {
    log.stream({ offsets: false }).pipe(
      push.collect((err, ary) => {
        t.error(err, 'no error when streaming compacted log')
        t.deepEqual(ary, [buf2, buf3], 'only 2nd and 3rd records exist')
        resolve()
      })
    )
  })

  await run(log.close)()
  t.end()
})

tape('del during compaction is forbidden', async (t) => {
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

  log.compact({}, (err) => {
    t.error(err, 'no error when compacting')
  })
  const [err, offset3] = await run(log.del)(10)
  t.ok(err, 'del is forbidden')
  t.match(err.message, /Cannot delete/)
  t.notOk(offset3)
  await run(log.onDrain)()

  await new Promise((resolve) => {
    log.stream({ offsets: false }).pipe(
      push.collect((err, ary) => {
        t.error(err, 'no error when streaming compacted log')
        t.deepEqual(ary, [buf2], 'only 2nd record exists')
        resolve()
      })
    )
  })

  await run(log.close)()
  t.end()
})
