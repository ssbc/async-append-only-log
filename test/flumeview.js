var Flume = require('flumedb')
var Index = require('flumeview-level')
var codec = require('flumecodec')
var fs = require('fs')

var create = require('..')

var toCompat = require('../compat')

require('test-flumeview-index')(function (filename, seed) {
  if (!fs.existsSync(filename))
    fs.mkdirSync(filename)

  var raf = create(filename + '/aligned.log', {
    blockSize: 1024*64,
    codec: require('flumecodec/json')
  })

  return Flume(toCompat(raf))
    .use('index', Index(1, function (e) {
      return [e.key]
    }))
})














