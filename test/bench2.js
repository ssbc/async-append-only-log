var FlumeLog = require('../')
var codec = require('flumecodec')
var toCompat = require('../compat')

var file = '/tmp/bench-async-flumelog.log'
try { require('fs').unlinkSync(file) } catch (_) {}

require('bench-flumelog')(function () {
  var log = FlumeLog(file, {
    block: 1024*64,
  })
  return toCompat(log)
}, null, null, function (obj) {
  return obj
})












