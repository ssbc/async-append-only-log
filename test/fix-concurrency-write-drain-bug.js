// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const tape = require('tape')
const fs = require('fs')
const Offset = require('../')

const file = '/tmp/ds-test_drain_since.log'

const msg1 = { text: 'hello world hello world' }

tape('check since after drain', async (t) => {
  for (var i = 0; i < 1000; ++i) {
    try {
      fs.unlinkSync(file + i)
    } catch (_) {}
    const db = Offset(file + i, {
      block: 16 * 1024,
      writeTimeout: 1,
      codec: require('flumecodec/json'),
    })

    await new Promise((resolve, reject) => {
      db.onReady(() => {
        db.append(msg1, (err, offset1) => {
          if (err) reject(err)

          setTimeout(() => {
            db.onDrain(() => {
              if (db.since.value !== 0) {
                t.fail('after drain offset was not set')
              }
              resolve()
            })
          }, 1)
        })
      })
    })
  }
  t.end()
})
