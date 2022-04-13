const Log = require('../')
const path = require('path')
const bipf = require('bipf')
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
        const deleteCount = Math.ceil(recordCount * 0.8)
        console.log('deleteCount', deleteCount)

        const deletableSeqs = new Map()
        while (deletableSeqs.size < deleteCount) {
          const candidateSeq = Math.floor(Math.random() * recordCount)
          if (deletableSeqs.has(candidateSeq)) continue
          deletableSeqs.set(candidateSeq, -1)
        }

        let seq = 1
        let sink = null
        log.stream({ values: false }).pipe(
          (sink = {
            paused: false,
            write(offset) {
              if (deletableSeqs.has(seq)) {
                deletableSeqs.set(seq, offset)
                // sink.paused = true
                const s = seq
                log.del(offset, (err) => {
                  if (err) throw err
                  // sink.paused = false
                  // sink.source.resume()
                  // if (Math.random() < 0.04) {
                  // console.log(
                  //   'progress',
                  //   ((100 * s) / recordCount).toFixed(2) + '%'
                  // )
                  // }
                })
              }
              seq += 1
            },
            end(err) {},
          })
        )
      }
    )
  )
})
