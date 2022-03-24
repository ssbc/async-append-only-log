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
  let deleteCount = 0
  log.stream({ offsets: true, values: true }).pipe(
    push.drain(
      (rec) => {
        recordCount += 1
        if (!rec.value) {
          deleteCount += 1
          // console.log('record ' + recordCount + ' deleted at offset ' + rec.offset)
        }
      },
      () => {
        console.log('recordCount', recordCount)
        console.log('deleteCount', deleteCount)
      }
    )
  )
})
