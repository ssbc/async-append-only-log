const Log = require('../')
const path = require('path')
const push = require('push-stream')

const logFilename = path.join(__dirname, 'log.bipf')

const log = Log(logFilename, {
  blockSize: 64 * 1024,
  validateRecord: (d) => {
    try {
      bipf.decode(d, 0)
      return true
    } catch (ex) {
      return false
    }
  },
})

log.since((maxOffset) => {
  console.log('maxOffset', maxOffset)
  let recordCount = 0
  log.stream({ values: false }).pipe(
    push.drain(
      () => {
        recordCount += 1
      },
      () => {
        console.log('recordCount', recordCount)
        const deleteCount = Math.ceil(recordCount * 0.35)
        console.log('deleteCount', deleteCount)

        const deletableSeqs = new Map()
        while (deletableSeqs.size < deleteCount) {
          const candidateSeq = Math.floor(Math.random() * recordCount * 0.4)
          if (deletableSeqs.has(candidateSeq)) continue
          deletableSeqs.set(candidateSeq, -1)
        }

        let seq = 1
        let sink = null
        console.time('make holes')
        log.stream({ values: false }).pipe(
          (sink = {
            paused: false,
            write(offset) {
              if (offset / maxOffset < 0.4) {
              // if (deletableSeqs.has(seq)) {
                //deletableSeqs.set(seq, offset)
                sink.paused = true
                // console.time('delete ' + offset)
                log.del(offset, (err) => {
                  if (err) throw err
                  sink.paused = false
                  // console.timeEnd('delete ' + offset)
                  sink.source.resume()
                })
              }
              seq += 1
            },
            end(err) {
              console.timeEnd('make holes')
            },
          })
        )
      }
    )
  )
})
