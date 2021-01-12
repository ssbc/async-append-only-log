var tape = require('tape')
var fs = require('fs')
var Log = require('../')

tape('NaN', function (t) {
  var file = '/tmp/dsf-test-bad-offset.log'
  try { fs.unlinkSync(file) } catch (_) {}
  var db = Log(file, {blockSize: 2*1024})

  var v1 = Buffer.from('testing')
  
  db.append(v1, function (err, offset1) {
    if(err) throw err
    t.equal(offset1, 0)
    db.get(NaN, function (err, b) {
      t.equal(err, 'Offset is not a number!')
      db.close(t.end)
    })
  })
})
