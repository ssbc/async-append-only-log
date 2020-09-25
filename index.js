const Cache = require('hashlru')
const RAF = require('polyraf')
const Obv = require('obz')
const debounce = require('lodash.debounce')
const debug = require('debug')("async-flumelog")

const Stream = require("./stream")

function id(e) { return e }
var _codec = {encode: id, decode: id, buffer: true}

module.exports = function (filename, opts) {
  var cache = new Cache(1024) // this is potentially 65mb!
  var raf = RAF(filename)
  var blockSize = opts && opts.blockSize || 65536
  var codec = opts && opts.codec || _codec
  var writeTimeout = opts && opts.writeTimeout || 250
  var self

  // offset of last written record
  var since = Obv()

  var waiting = [], waitingDrain = []
  var blocksToBeWritten = new Map() // blockIndex -> { block, fileOffset }

  var latestBlock = null
  var latestBlockIndex = null
  var nextWriteBlockOffset = null

  raf.stat(function (_, stat) {
    var len = stat ? stat.size : -1

    if (len <= 0) {
      debug("empty file")
      latestBlock = Buffer.alloc(blockSize)
      latestBlockIndex = 0
      nextWriteBlockOffset = 0
      cache.set(0, latestBlock)
      since.set(-1)
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

  function getNextBlockIndex(offset) {
    return (getBlockIndex(offset) + 1) * blockSize
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
      // we use setImmediate to avoid the stack blowing up
      setImmediate(() => cb(null, cachedBlock))
    } else {
      debug("getting offset %d from disc", offset)
      raf.read(blockStart, blockSize, (err, buffer) => {
        cache.set(getBlockIndex(offset), buffer)
        cb(err, buffer)
      })
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
      getData(buffer, getRecordOffset(offset), cb)
    })
  }

  // nextOffset can take 3 values:
  // -1: end of stream
  //  0: need a new block
  // >0: next record within block
  function getDataNextOffset(buffer, offset) {
    const recordOffset = getRecordOffset(offset)
    const blockIndex = getBlockIndex(offset)

    const length = buffer.readUInt16LE(recordOffset)
    const data = buffer.slice(recordOffset + 2, recordOffset + 2 + length)

    const nextLength = buffer.readUInt16LE(recordOffset + 2 + length)
    let nextOffset = recordOffset + 2 + length + blockIndex * blockSize
    if (nextLength == 0 && getNextBlockIndex(offset) > since.value)
      nextOffset = -1
    else if (nextLength == 0)
      nextOffset = 0

    if (data.every(x => x === 0))
      return [nextOffset, null]
    else
      return [nextOffset, codec.decode(data)]
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

  function appendSingle(data) {
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
      debug("data doesn't fit current block, creating new")
    }
    
    appendFrame(latestBlock, encodedData, nextWriteBlockOffset)
    cache.set(latestBlockIndex, latestBlock) // update cache
    const fileOffset = nextWriteBlockOffset + latestBlockIndex * blockSize
    nextWriteBlockOffset += frameSize(encodedData)
    blocksToBeWritten[latestBlockIndex] = { block: latestBlock, fileOffset }
    scheduleWrite()
    debug("data inserted at offset %d", fileOffset)
    return fileOffset
  }

  function append(data, cb)
  {
    if (Array.isArray(data)) {
      var fileOffset = 0
      for (var i = 0, length = data.length; i < length; ++i)
        fileOffset = appendSingle(data[i])

      cb(null, fileOffset)
    } else
      cb(null, appendSingle(data))
  }

  scheduleWrite = debounce(write, writeTimeout)

  function writeBlock(blockIndex) {
    const { block, fileOffset } = blocksToBeWritten[blockIndex]
    delete blocksToBeWritten[blockIndex]
    debug("writing block of size: %d, to offset: %d",
          block.length, blockIndex * blockSize)
    raf.write(blockIndex * blockSize, block, (err) => {
      if (err) {
        debug("failed to write block %d", blockIndex)
        throw err
      } else {
        since.set(fileOffset)

        // write values to live streams
        self.streams.forEach(stream => {
          if (!stream.ended && stream.live && !stream.writing) {
            if (stream.cursor === -1)
              stream.cursor = 0
            else if (getNextBlockIndex(stream.cursor) < since.value)
              stream.cursor = getNextBlockIndex(stream.cursor)

            stream.writing = true
            stream.resume()
          }
        })

        var l = waitingDrain.length
        for (var i = 0; i < l; ++i)
          waitingDrain[i]()
        waitingDrain = waitingDrain.slice(l)

        write() // next!
      }
    })
  }

  function write() {
    for (var blockIndex in blocksToBeWritten) {
      writeBlock(blockIndex)
      return // just one at a time
    }
  }

  function close(cb) {
    self.onDrain(function () {
      while (self.streams.length)
        self.streams.shift().abort(new Error('async-flumelog: closed'))
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

    filename,

    // streaming
    getNextBlockIndex,
    getDataNextOffset,
    getBlock,
    stream: function (opts) {
      var stream = new Stream(self, opts)
      self.streams.push(stream)
      return stream
    },
    streams: [],
  }
}
