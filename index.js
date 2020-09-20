const Cache = require('lru_cache').LRUCache
const RAF = require('polyraf')
const Obv = require('obv')
const debug = require('debug')("ds-flumelog")

const Stream = require("./stream")

function id(e) { return e }
var _codec = {encode: id, decode: id, buffer: true}

module.exports = function (file, opts) {
  var cache = new Cache(1024) // this is potentially 65mb!
  var raf = RAF(file)
  var blockSize = opts && opts.blockSize || 65536
  var codec = opts && opts.codec || _codec
  var writeTimeout = opts && opts.writeTimeout || 250

  // offset of last written record
  var since = Obv()

  var waiting = [], waitingDrain = []
  var blocksToBeWritten = {} // blockIndex -> { block, fileOffset }

  var latestBlock = null
  var latestBlockIndex = null
  var nextWriteBlockOffset = null

  raf.stat(function (_, stat) {
    var len = stat ? stat.size : -1
    
    if (len == -1) {
      debug("empty file")
      latestBlock = Buffer.alloc(blockSize)
      latestBlockIndex = 0
      nextWriteBlockOffset = 0
      cache.set(0, latestBlock)
      since.set(0)
      while(waiting.length) waiting.shift()()
    } else {
      raf.read(len - blockSize, blockSize, (err, buffer) => {
        if (err) throw err
        
        var recordOffset = getLastRecord(buffer, 0)
        since.set(len - blockSize + recordOffset)

        latestBlock = buffer
        var recordLength = buffer.readUInt16LE(recordOffset)
        nextWriteBlockOffset = recordOffset + 2 + recordLength
        latestBlockIndex = len / blockSize - 1

        debug("opened file, since: %d", since.value)

        while(waiting.length) waiting.shift()()
      })
    }
  })

  function getRecordOffset(offset) {
    return offset % blockSize
  }

  function getBlockIndex(offset) {
    return (offset - getRecordOffset(offset)) / blockSize
  }

  function getLastRecord(buffer) {
    for (var i = 0, lastOk = 0; i < buffer.length;) {
      var length = buffer.readUInt16LE(i)
      if (length == 0)
        break
      else {
        lastOk = i
        i += 2 + length
      }
    }

    return lastOk
  }
  
  function getBlock(offset, cb) {
    var blockStart = offset - getRecordOffset(offset)
    var blockIndex = blockStart / blockSize

    var cachedBlock = cache.get(blockIndex)
    if (cachedBlock) {
      debug("getting offset %d from cache", offset)
      cb(null, cachedBlock)
    } else {
      debug("getting offset %d from disc", offset)
      raf.read(blockStart, blockSize, cb)
    }
  }

  function getData(buffer, recordOffset, cb) {
    var length = buffer.readUInt16LE(recordOffset)
    var data = buffer.slice(recordOffset + 2, recordOffset + 2 + length)

    if (data.every(x => x === 0)) {
      const err = new Error('item has been deleted')
      err.code = 'flumelog:deleted'
      return cb(err)
    }
    else
      cb(null, codec.decode(data))
  }

  function get(offset, cb) {
    getBlock(offset, (err, buffer) => {
      if (err) return cb(err)
      cache.set(getBlockIndex(offset), buffer)
      getData(buffer, getRecordOffset(offset), cb)
    })
  }

  // stream skips deleted stuff
  function getDataNextOffset(buffer, recordOffset, blockIndex) {
    var length = buffer.readUInt16LE(recordOffset)
    var data = buffer.slice(recordOffset + 2, recordOffset + 2 + length)

    var nextLength = buffer.readUInt16LE(recordOffset + 2 + length)
    var nextOffset = nextLength != 0 ? recordOffset + 2 + length : (blockIndex + 1) * blockSize
    if (nextOffset > since.value)
      nextOffset = -1

    return [nextOffset, codec.decode(data)]
  }

  function getNext(offset, cb) {
    getBlock(offset, (err, buffer) => {
      if (err) return cb(err)
      const blockIndex = getBlockIndex(offset)
      cache.set(blockIndex, buffer)
      cb(null, getDataNextOffset(buffer, getRecordOffset(offset), blockIndex))
    })
  }
  
  function del(offset, cb)
  {
    getBlock(offset, (err, buffer) => {
      if (err) return cb(err)

      const recordOffset = getRecordOffset(offset)
      const recordLength = buffer.readUInt16LE(recordOffset)
      const nullBytes = Buffer.alloc(recordLength)
      nullBytes.copy(buffer, recordOffset+2)

      // we write directly here to make normal write simpler
      raf.write(offset - recordOffset, buffer, cb)
    })
  }

  function appendFrame(buffer, data, offset)
  {
    buffer.writeUInt16LE(data.length, offset)
    data.copy(buffer, offset+2)
  }

  function frameSize(buffer)
  {
    return buffer.length + 2
  }
  
  function append(data, cb)
  {
    // FIXME: support sending an array of data

    let encodedData = codec.encode(data)
    if (typeof encodedData == 'string')
      encodedData = Buffer.from(encodedData)

    if (frameSize(encodedData) + 2 > blockSize)
      throw new Error("data larger than block size")

    if (nextWriteBlockOffset + frameSize(encodedData) + 2 > blockSize)
    {
      // doesn't fit
      var buffer = Buffer.alloc(blockSize)
      latestBlock = buffer
      latestBlockIndex += 1
      nextWriteBlockOffset = 0
      cache.set(latestBlockIndex, latestBlock)
      debug("data doesn't fit current block, creating new")
    }
    
    appendFrame(latestBlock, encodedData, nextWriteBlockOffset)
    const fileOffset = nextWriteBlockOffset + latestBlockIndex * blockSize
    nextWriteBlockOffset += frameSize(encodedData)
    blocksToBeWritten[latestBlockIndex] = { block: latestBlock, fileOffset }
    scheduleWrite()
    debug("data inserted at offset %d", fileOffset)
    cb(null, fileOffset)
  }

  function scheduleWrite() {
    // FIXME: debounce this
    setTimeout(write, writeTimeout)
  }

  function write() {
    // FIXME: does this need to be sorted?
    for (var blockIndex in blocksToBeWritten)
    {
      const { block, fileOffset } = blocksToBeWritten[blockIndex]
      delete blocksToBeWritten[blockIndex]
      debug("writing block of size: %d, to offset: %d",
            block.length, blockIndex * blockSize)
      raf.write(blockIndex * blockSize, block, (err) => {
        if (err)
          debug("failed to write block %d", blockIndex)
        else {
          debug("wrote block %d", blockIndex)
          since.set(fileOffset)

          var l = waitingDrain.length
          for (var i = 0; i < l; ++i)
            waitingDrain[i]()
          waitingDrain = waitingDrain.slice(l)
        }
      })
    }
  }

  function close(cb) {
    self.onDrain(function () {
      while(self.streams.length)
        self.streams.shift().abort(new Error('ds-flumelog: closed'))
      raf.close(cb)
    })
  }
  
  function onLoad (fn) {
    return function (arg, cb) {
      if (latestBlock === null)
        waiting.push(function () { fn(arg, cb) })
      else fn(arg, cb)
    }
  }

  function onReady(fn) {
    if (latestBlock != null) fn()
    else waiting.push(fn)
  }
  
  return self = {
    get: onLoad(get),
    del: onLoad(del),
    append: onLoad(append),
    close: onLoad(close),
    since,
    onReady,

    onDrain: onLoad(function (fn) {
      if (Object.keys(blocksToBeWritten).length == 0) fn()
      else waitingDrain.push(fn)
    }),

    // streaming
    getNext,
    stream: function (opts) {
      var stream = new Stream(this, opts)
      this.streams.push(stream)
      return stream
    },
    streams: [],
  }
}
