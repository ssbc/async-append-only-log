const Cache = require('hashlru')
const RAF = require('polyraf')
const Obv = require('obz')
const debounce = require('lodash.debounce')
const debug = require('debug')("async-flumelog")

const Stream = require("./stream")

function alwaysTrue() { return true }
function id(e) { return e }
var _codec = {encode: id, decode: id, buffer: true}

module.exports = function (filename, opts) {
  var cache = new Cache(1024) // this is potentially 65mb!
  var raf = RAF(filename)
  var blockSize = opts && opts.blockSize || 65536
  var codec = opts && opts.codec || _codec
  var writeTimeout = opts && opts.writeTimeout || 250
  var validateRecord = opts && opts.validateRecord || alwaysTrue
  var self

  // offset of last written record
  var since = Obv()

  var waiting = []
  const waitingDrain = new Map() // blockIndex -> []
  const blocksToBeWritten = new Map() // blockIndex -> { block, fileOffset }
  let writingBlockIndex = -1

  var latestBlock = null
  var latestBlockIndex = null
  var nextWriteBlockOffset = null

  raf.stat(function (err, stat) {
    if (err) debug("failed to stat " + filename, err)

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

        getLastGoodRecord(buffer, len - blockSize, (err, recordOffset) => {
          since.set(len - blockSize + recordOffset)

          latestBlock = buffer
          var recordLength = buffer.readUInt16LE(recordOffset)
          nextWriteBlockOffset = recordOffset + 2 + recordLength
          latestBlockIndex = len / blockSize - 1

          debug("opened file, since: %d", since.value)

          while(waiting.length) waiting.shift()()
        })
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

  function fixBlock(buffer, i, offset, lastOk, cb) {
    debug("found record that does not validate, fixing last block", i)

    var goodData = buffer.slice(0, i)
    const newBlock = Buffer.alloc(blockSize)
    goodData.copy(newBlock, 0)

    raf.write(offset, newBlock, () => {
      cb(null, lastOk)
    })
  }

  function getLastGoodRecord(buffer, offset, cb) {
    for (var i = 0, lastOk = 0; i < buffer.length;) {
      var length = buffer.readUInt16LE(i)
      if (length === 0)
        break
      else {
        if (i + 2 + length > blockSize) {
          // corrupt length data
          return fixBlock(buffer, i, offset, lastOk, cb)
        } else {
          var data = buffer.slice(i + 2, i + 2 + length)
          if (validateRecord(data)) {
            lastOk = i
            i += 2 + length
          } else {
            // corrupt message data
            return fixBlock(buffer, i, offset, lastOk, cb)
          }
        }
      }
    }

    cb(null, lastOk)
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
    if (typeof offset !== 'number' || isNaN(offset))
      return cb(`Offset ${offset} is not a number`)
    else if (offset < 0)
      return cb(`Offset is ${offset} must be >= 0`)

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
    if (nextLength === 0 && getNextBlockIndex(offset) > since.value)
      nextOffset = -1
    else if (nextLength === 0)
      nextOffset = 0

    if (data.every(x => x === 0))
      return [nextOffset, null]
    else
      return [nextOffset, codec.decode(data)]
  }

  function del(offset, cb) {
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

  function appendFrame(buffer, data, offset) {
    buffer.writeUInt16LE(data.length, offset)
    data.copy(buffer, offset+2)
  }

  function frameSize(buffer) {
    return buffer.length + 2
  }

  function appendSingle(data) {
    let encodedData = codec.encode(data)
    if (typeof encodedData === 'string')
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
    blocksToBeWritten.set(latestBlockIndex, { block: latestBlock, fileOffset })
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

  var scheduleWrite = debounce(write, writeTimeout)

  function writeBlock(blockIndex) {
    if (!blocksToBeWritten.has(blockIndex)) return
    writingBlockIndex = blockIndex
    const { block, fileOffset } = blocksToBeWritten.get(blockIndex)
    blocksToBeWritten.delete(blockIndex)

    debug("writing block of size: %d, to offset: %d",
          block.length, blockIndex * blockSize)
    raf.write(blockIndex * blockSize, block, (err) => {
      const drainsBefore = (waitingDrain.get(blockIndex) || []).slice(0)
      writingBlockIndex = -1
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
            else // the cursor still at last position
              stream.skipFirst = true

            stream.writing = true
            stream.resume()
          }
        })

        debug("draining the waiting queue for %d, items: %d", blockIndex, drainsBefore.length)
        for (var i = 0; i < drainsBefore.length; ++i)
          drainsBefore[i]()

        // the resumed streams might have added more to waiting
        let drainsAfter = waitingDrain.get(blockIndex) || []
        if (drainsBefore.length === drainsAfter.length)
          waitingDrain.delete(blockIndex)
        else if (drainsAfter.length === 0)
          waitingDrain.delete(blockIndex)
        else
          waitingDrain.set(blockIndex, waitingDrain.get(blockIndex).slice(drainsBefore.length))

        write() // next!
      }
    })
  }

  function write() {
    // just one at a time
    if (blocksToBeWritten.size > 0)
      writeBlock(blocksToBeWritten.keys().next().value)
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

  function last(iterable) {
    let res = null
    for (let x of iterable) res = x
    return res
  }

  return self = {
    get: onLoad(get),
    del: onLoad(del),
    append: onLoad(append),
    close: onLoad(close),
    since,
    onReady,

    onDrain: onLoad(function (fn) {
      if (blocksToBeWritten.size === 0 && writingBlockIndex === -1) fn()
      else {
        const latestBlockIndex = blocksToBeWritten.size > 0 ? last(blocksToBeWritten.keys()) : writingBlockIndex
        const drains = waitingDrain.get(latestBlockIndex) || []
        drains.push(fn)
        waitingDrain.set(latestBlockIndex, drains)
      }
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
