const Log = require('../')
const path = require('path')

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

log.onDrain(() => {
  console.time('truncate')
  log.truncate(1526, (err) => {
    if (err) throw err
    console.timeEnd('truncate')
  })
})
