var toPull = require('push-stream-to-pull-stream/source')

module.exports = function toCompat(log) {
  log.onWrite = log.since.set

  var _stream = log.stream
  log.stream = function (opts) {
    var stream = _stream.call(log, opts)
    return toPull(stream)
  }
  return log
}

