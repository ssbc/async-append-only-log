const tape = require('tape')
const fs = require('fs')
const bipf = require('bipf')
const RAF = require('polyraf')
const Log = require('../')

function toBIPF(msg) {
  const len = bipf.encodingLength(msg)
  const buf = Buffer.alloc(len)
  bipf.encode(msg, buf, 0)
  return buf
}

tape('corrupt message', function (t) {
  var file = '/tmp/jacob.log'
  try { fs.unlinkSync(file) } catch (_) {}
  var db = Log(file, {blockSize: 64*1024})

  var bipf1 = toBIPF({ text: 'testing' })
  var bipf2 = toBIPF({ bool: true, test: 'testing2' })
  bipf2[7] = '!' // corrupt the message
  
  db.append(bipf1, function (err, offset1) {
    if(err) throw err
    db.append(bipf2, function (err, offset2) {
      if(err) throw err

      db.close(t.end)
    })
  })
})

tape('corrupt message re-read without validation', function (t) {
  var file = '/tmp/jacob.log'
  var db = Log(file, {blockSize: 64*1024})
  
  db.onReady(() => {
    var result = []
    
    db.stream({seqs: false}).pipe({
      paused: false,
      write: function (e) { result.push(e) },
      end: function() {
        // because these are just buffers we won't see the corruption
        t.equal(result.length, 2)
        db.close(t.end)
      }
    })
  })
})

tape('corrupt message re-read with validation', function (t) {
  var file = '/tmp/jacob.log'
  var db = Log(file, {
    blockSize: 64*1024,
    validateRecord: (d) => {
      try {
        bipf.decode(d, 0)
        return true
      } catch (ex) {
        return false
      }
    }
  })
  
  db.onReady(() => {
    var result = []
    
    db.stream({seqs: false}).pipe({
      paused: false,
      write: function (e) { result.push(e) },
      end: function() {
        t.equal(result.length, 1)
        db.close(t.end)
      }
    })
  })
})

tape('length corruption', function (t) {
  let file = '/tmp/jacob-length.log'
  try { fs.unlinkSync(file) } catch (_) {}

  var raf = RAF(file)
  let block = Buffer.alloc(64*1024)

  const bipf1 = toBIPF({ text: 'testing' })
  const bipf2 = toBIPF({ bool: true, test: 'testing2' })

  block.writeUInt16LE(bipf1.length, 0)
  bipf1.copy(block, 2)
  block.writeUInt16LE(65534, 2+bipf1.length)
  bipf2.copy(block, 2+bipf1.length+2)
  
  raf.write(0, block, (err) => {
    raf.close(t.end())
  })
})

tape('length re-read without validation', function (t) {
  var file = '/tmp/jacob-length.log'
  var db = Log(file, {
    blockSize: 64*1024
  })
  
  db.onReady(() => {
    var result = []
    
    db.stream({seqs: false}).pipe({
      paused: false,
      write: function (e) { result.push(e) },
      end: function() {
        t.equal(result.length, 1)

        // append a fixed record
        const bipf2 = toBIPF({ bool: true, test: 'testing2' })
        db.append(bipf2, function (err) {
          t.error(err)
          db.close(t.end)
        })
      }
    })
  })
})

tape('length re-read with validation', function (t) {
  var file = '/tmp/jacob-length.log'
  var db = Log(file, {
    blockSize: 64*1024,
    validateRecord: (d) => {
      try {
        bipf.decode(d, 0)
        return true
      } catch (ex) {
        return false
      }
    }
  })

  db.onReady(() => {
    var result = []
    
    db.stream({seqs: false}).pipe({
      paused: false,
      write: function (e) { result.push(e) },
      end: function() {
        t.equal(result.length, 2)
        db.close(t.end)
      }
    })
  })
})
