const Cache = require('lru_cache').LRUCache
const RAF = require('polyraf')
const Obv = require('obv')
const debug = require('debug')("dead-simple-flumelog")

const Stream = require("./stream")

function id(e) { return e }
var _codec = {encode: id, decode: id, buffer: true}

// A block consists of a number of records
// A record is length + data
// After the last record in a block there will be a EOB marker
// To know if there is another block after the current, you need the
// file length

module.exports = function (file, opts) {
  var cache = new Cache(1024) // this is 65mb!
  const EOBMarker = 0
  var raf = RAF(file)
  var blockSize = opts && opts.blockSize || 65536
  var codec = opts && opts.codec || _codec
  var writeTimeout = opts && opts.writeTimeout || 1000
  var since = Obv()
  var blocksToBeWritten = {} // index -> block

  var latestBlock = null
  var latestBlockIndex = null
  var offsetInLatestBlock = null

  var self
  
  raf.stat(function (_, stat) {
    var len = stat ? stat.size : -1
    self.length = length = len == -1 ? 0 : len
    
    if (len == -1) {
      debug("empty file")
      latestBlock = Buffer.alloc(blockSize)
      latestBlockIndex = 0
      offsetInLatestBlock = 0
      cache.set(0, latestBlock)
      while(waiting.length) waiting.shift()()
    } else {
      // data will always be written in block sizes
      raf.read(len - blockSize, blockSize, (err, buffer) => {
        if (err) throw err
        
        var recordOffset = getLastRecord(buffer, 0)
        since.set(len - blockSize + recordOffset)

        latestBlock = buffer
        var recordLength = buffer.readUInt16LE(recordOffset)
        offsetInLatestBlock = recordOffset + 2 + recordLength
        latestBlockIndex = len / blockSize - 1

        debug("opened file, since: %d", since.value)

        while(waiting.length) waiting.shift()()
      })
    }
  })

  // FIXME: this is wrong
  function getLastRecord(buffer, i) {
    var length = buffer.readUInt16LE(i)
    if (length == 0)
      return i
    else
      return getLastRecord(buffer, i + 2 + length)
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
    var recordOffset = offset % blockSize
    var blockStart = offset - recordOffset
    var blockIndex = blockStart / blockSize

    var cachedBlock = cache.get(blockIndex)
    if (cachedBlock) {
      debug("getting offset %d from cache", offset)
      getData(cachedBlock, recordOffset, cb)
    } else {
      debug("getting offset %d from disc", offset)
      raf.read(blockStart, blockSize, (err, buffer) => {
        if (err) return cb(err)
        cache.set(blockIndex, buffer)
        getData(buffer, recordOffset, cb)
      })
    }
  }

  // stream just skips deleted stuff
  function getDataNextOffset(buffer, recordOffset, blockIndex) {
    var length = buffer.readUInt16LE(recordOffset)
    var data = buffer.slice(recordOffset + 2, recordOffset + 2 + length)

    var nextLength = buffer.readUInt16LE(recordOffset + 2 + length)
    var nextOffset = nextLength != 0 ? recordOffset + 2 + length : (blockIndex + 1) * blockSize

    return [nextOffset, codec.decode(data)]
  }

  // FIXME: refactor me
  function getNext(offset, cb) {
    var recordOffset = offset % blockSize
    var blockStart = offset - recordOffset
    var blockIndex = blockStart / blockSize

    var cachedBlock = cache.get(blockIndex)
    if (cachedBlock) {
      debug("getting offset %d from cache", offset)
      cb(getDataNextOffset(cachedBlock, recordOffset, blockIndex))
    } else {
      debug("getting offset %d from disc", offset)
      raf.read(blockStart, blockSize, (err, buffer) => {
        if (err) return cb(err)
        cache.set(blockIndex, buffer)
        cb(getDataNextOffset(buffer, recordOffset, blockIndex))
      })
    }
  }
  
  function nullMessageInBlock(buffer, recordOffset)
  {
    var length = buffer.readUInt16LE(recordOffset)
    const nullBytes = Buffer.alloc(length)
    nullBytes.copy(buffer, recordOffset+2)
  }
  
  function del(offset, cb)
  {
    var recordOffset = offset % blockSize
    var blockStart = offset - recordOffset
    var blockIndex = blockStart / blockSize
    
    var cachedBlock = cache.get(blockIndex)
    if (cachedBlock) {
      nullMessageInBlock(cachedBlock, recordOffset)
      blocksToBeWritten[blockIndex] = cachedBlock
      cb(null, cachedBlock)
    }
    else
      raf.read(blockStart, blockSize, (err, buffer) => {
        if (err) return cb(err)
        nullMessageInBlock(buffer, recordOffset)
        cache.set(blockIndex, buffer)
        blocksToBeWritten[blockIndex] = buffer
        cb(null, buffer)
      })
  }

  function appendFrame(buffer, data, offset)
  {
    // length
    buffer.writeUInt16LE(data.length, offset)
    // data
    data.copy(buffer, offset+2)
    // EOB marker
    buffer.writeUInt16LE(EOBMarker, offset + data.length + 2)
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

    if (offsetInLatestBlock + frameSize(encodedData) + 2 > blockSize)
    {
      // doesn't fit
      var buffer = Buffer.alloc(blockSize)
      latestBlock = buffer
      latestBlockIndex += 1
      offsetInLatestBlock = 0
      cache.set(latestBlockIndex, latestBlock)
      debug("data doesn't fit current block, creating new")
    }
    
    appendFrame(latestBlock, encodedData, offsetInLatestBlock)
    const fileOffset = offsetInLatestBlock + latestBlockIndex * blockSize
    offsetInLatestBlock += frameSize(encodedData)
    blocksToBeWritten[latestBlockIndex] = latestBlock
    debug("data inserted at offset %d", fileOffset)
    self.length += offsetInLatestBlock + latestBlockIndex * blockSize
    cb(null, fileOffset)
  }

  function scheduleWrite() {
    setTimeout(write, writeTimeout)
  }

  function write() {
    for (var i in blocksToBeWritten)
    {
      var block = blocksToBeWritten[i]
      delete blocksToBeWritten[i]
      debug("writing block of size %d to %d", block.length, i * blockSize)
      raf.write(i * blockSize, block, (err) => {
        if (err)
          debug("failed to write block %d", i)
        else
          debug("wrote block %d", i)
      })
    }
  }

  function close(cb) {
    write()
    // FIXME: close streams
    raf.close(cb)
  }
  
  var waiting = []
  
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

    getNext,

    stream: function (opts) {
      var stream = new Stream(this, opts)
      this.streams.push(stream)
      return stream
    },

    streams: [],
  }
}
