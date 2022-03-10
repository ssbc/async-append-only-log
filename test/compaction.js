const tape = require('tape')
const push = require('push-stream')
const Log = require('../')

tape('delete, compact, stream', (t) => {
  const file = '/tmp/compaction-test_' + Date.now() + '.log'
  const log = Log(file, { blockSize: 10 })

  const buf1 = Buffer.from('first')
  const buf2 = Buffer.from('second')

  log.append(buf1, (err, offset1) => {
    t.error(err, 'no error')
    log.append(buf2, (err, offset2) => {
      t.error(err, 'no error')
      log.onDrain(() => {
        log.del(offset1, (err) => {
          t.error(err, 'no error')
          log.onDrain(() => {
            log.compact({}, (err) => {
              t.error(err, 'no error')
              log.onDrain(() => {
                log.stream({ offsets: false }).pipe(
                  push.collect((err, ary) => {
                    t.error(err, 'no error')
                    t.deepEqual(ary, [buf2])
                    t.end()
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

tape.skip('delete the last record, then compact, then stream', (t) => {})

tape.skip('delete remaining blocks at the end after compaction', (t) => {
  // FIXME:
})
