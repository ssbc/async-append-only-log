// SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros <contact@staltz.com>
//
// SPDX-License-Identifier: CC0-1.0

const tape = require('tape')
const fs = require('fs')
const toPull = require('push-stream-to-pull-stream/source')
const pull = require('pull-stream')
const Log = require('../')

const filename = '/tmp/aaol-abort-live-pull-stream.log'

try {
  fs.unlinkSync(filename)
} catch (_) {}
const log = Log(filename, { blockSize: 64 * 1024 })

const msg1 = Buffer.alloc(10).fill(0x10)
const msg2 = Buffer.alloc(20).fill(0x20)
const msg3 = Buffer.alloc(30).fill(0x30)

tape('abort live push-stream-to-pull-stream should not end with err', (t) => {
  t.plan(8)
  log.append(msg1, (err) => {
    t.error(err, 'no err to append msg1')
    log.append(msg2, (err) => {
      t.error(err, 'no err to append msg2')
      const expected = [msg1, msg2, msg3]
      const logPushStream = log.stream({ live: true, offsets: false })
      const logPullStream = toPull(logPushStream)
      pull(
        logPullStream,
        pull.drain(
          (buf) => {
            t.deepEqual(buf, expected.shift())
            if (expected.length === 0) {
              log.close(() => {
                t.pass('closed AAOL')
              })
            }
          },
          (err) => {
            t.error(err, 'no err when pull.draining')
          }
        )
      )
    })
  })
  log.append(msg3, (err) => {
    t.error(err, 'no err to append msg3')
  })
})
