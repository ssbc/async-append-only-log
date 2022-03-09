// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const Cache = require('hashlru')
const RAF = require('polyraf')
const Obv = require('obz')
const debounce = require('lodash.debounce')
const debug = require('debug')('async-flumelog')
const fs = require('fs')
const mutexify = require('mutexify')

const Stream = require('./stream')

const DEFAULT_BLOCK_SIZE = 65536
const DEFAULT_CODEC = { encode: (x) => x, decode: (x) => x }
const DEFAULT_WRITE_TIMEOUT = 250
const DEFAULT_VALIDATE = () => true

module.exports = function (filename, opts) {
  const cache = new Cache(1024) // this is potentially 65mb!
  const raf = RAF(filename)
  const blockSize = (opts && opts.blockSize) || DEFAULT_BLOCK_SIZE
  const codec = (opts && opts.codec) || DEFAULT_CODEC
  const writeTimeout = (opts && opts.writeTimeout) || DEFAULT_WRITE_TIMEOUT
  const validateRecord = (opts && opts.validateRecord) || DEFAULT_VALIDATE
  let self

  // offset of last written record
  const since = Obv()

  const waiting = []
  const waitingDrain = new Map() // blockIndex -> []
  const blocksToBeWritten = new Map() // blockIndex -> { blockBuf, fileOffset }
  let writingBlockIndex = -1

  let latestBlockBuf = null
  let latestBlockIndex = null
  let nextWriteBlockOffset = null

  raf.stat(function (err, stat) {
    if (err) debug('failed to stat ' + filename, err)

    const len = stat ? stat.size : -1

    if (len <= 0) {
      debug('empty file')
      latestBlockBuf = Buffer.alloc(blockSize)
      latestBlockIndex = 0
      nextWriteBlockOffset = 0
      cache.set(0, latestBlockBuf)
      since.set(-1)
      while (waiting.length) waiting.shift()()
    } else {
      raf.read(len - blockSize, blockSize, (err, blockBuf) => {
        if (err) throw err

        getLastGoodRecord(blockBuf, len - blockSize, (err, recordOffset) => {
          since.set(len - blockSize + recordOffset)

          latestBlockBuf = blockBuf
          const recordLength = blockBuf.readUInt16LE(recordOffset)
          nextWriteBlockOffset = recordOffset + 2 + recordLength
          latestBlockIndex = len / blockSize - 1

          debug('opened file, since: %d', since.value)

          while (waiting.length) waiting.shift()()
        })
      })
    }
  })

  function getRecordOffset(offset) {
    return offset % blockSize
  }

  function getBlockStart(offset) {
    return offset - getRecordOffset(offset)
  }

  function getBlockIndex(offset) {
    return getBlockStart(offset) / blockSize
  }

  function getNextBlockIndex(offset) {
    return (getBlockIndex(offset) + 1) * blockSize
  }

  const writeLock = mutexify()

  function writeWithFSync(offset, blockBuf, successValue, cb) {
    writeLock((unlock) => {
      raf.write(offset, blockBuf, (err) => {
        if (err) return unlock(cb, err)

        if (raf.fd) {
          fs.fsync(raf.fd, (err) => {
            if (err) unlock(cb, err)
            else unlock(cb, null, successValue)
          })
        } else unlock(cb, null, successValue)
      })
    })
  }

  function fixBlock(blockBuf, i, offset, lastOk, cb) {
    debug('found record that does not validate, fixing last block', i)

    const goodData = blockBuf.slice(0, i)
    const newBlockBuf = Buffer.alloc(blockSize)
    goodData.copy(newBlockBuf, 0)

    writeWithFSync(offset, newBlockBuf, lastOk, cb)
  }

  function getLastGoodRecord(blockBuf, offset, cb) {
    let lastOk = 0
    for (let i = 0; i < blockBuf.length; ) {
      const length = blockBuf.readUInt16LE(i)
      if (length === 0) break
      else {
        if (i + 2 + length > blockSize) {
          // corrupt length data
          return fixBlock(blockBuf, i, offset, lastOk, cb)
        } else {
          const data = blockBuf.slice(i + 2, i + 2 + length)
          if (validateRecord(data)) {
            lastOk = i
            i += 2 + length
          } else {
            // corrupt message data
            return fixBlock(blockBuf, i, offset, lastOk, cb)
          }
        }
      }
    }

    cb(null, lastOk)
  }

  function getBlock(offset, cb) {
    const blockStart = getBlockStart(offset)
    const blockIndex = getBlockIndex(offset)

    const cachedBlockBuf = cache.get(blockIndex)
    if (cachedBlockBuf) {
      debug('getting offset %d from cache', offset)
      cb(null, cachedBlockBuf)
    } else {
      debug('getting offset %d from disc', offset)
      raf.read(blockStart, blockSize, (err, blockBuf) => {
        cache.set(blockIndex, blockBuf)
        cb(err, blockBuf)
      })
    }
  }

  function getData(blockBuf, recordOffset, cb) {
    const length = blockBuf.readUInt16LE(recordOffset)
    const data = blockBuf.slice(recordOffset + 2, recordOffset + 2 + length)

    if (data.every((x) => x === 0)) {
      const err = new Error('item has been deleted')
      err.code = 'flumelog:deleted'
      return cb(err)
    } else cb(null, codec.decode(data))
  }

  function get(offset, cb) {
    if (typeof offset !== 'number' || isNaN(offset))
      return cb(`Offset ${offset} is not a number`)
    else if (offset < 0) return cb(`Offset is ${offset} must be >= 0`)

    getBlock(offset, (err, blockBuf) => {
      if (err) return cb(err)
      getData(blockBuf, getRecordOffset(offset), cb)
    })
  }

  // nextOffset can take 3 values:
  // -1: end of stream
  //  0: need a new block
  // >0: next record within block
  function getDataNextOffset(blockBuf, offset) {
    const recordOffset = getRecordOffset(offset)
    const blockIndex = getBlockIndex(offset)

    const length = blockBuf.readUInt16LE(recordOffset)
    const data = blockBuf.slice(recordOffset + 2, recordOffset + 2 + length)

    const nextLength = blockBuf.readUInt16LE(recordOffset + 2 + length)
    let nextOffset = recordOffset + 2 + length + blockIndex * blockSize
    if (nextLength === 0 && getNextBlockIndex(offset) > since.value)
      nextOffset = -1
    else if (nextLength === 0) nextOffset = 0

    if (data.every((x) => x === 0)) return [nextOffset, null]
    else return [nextOffset, codec.decode(data)]
  }

  function del(offset, cb) {
    getBlock(offset, (err, blockBuf) => {
      if (err) return cb(err)

      const recordOffset = getRecordOffset(offset)
      const recordLength = blockBuf.readUInt16LE(recordOffset)
      blockBuf.fill(0, recordOffset + 2, recordOffset + 2 + recordLength)

      // we write directly here to make normal write simpler
      writeWithFSync(offset - recordOffset, blockBuf, null, cb)
    })
  }

  function appendRecord(blockBuf, data, offset) {
    blockBuf.writeUInt16LE(data.length, offset)
    data.copy(blockBuf, offset + 2)
  }

  function recordSize(dataBuf) {
    return dataBuf.length + 2
  }

  function appendSingle(data) {
    let encodedData = codec.encode(data)
    if (typeof encodedData === 'string') encodedData = Buffer.from(encodedData)

    // we always leave 2 bytes at the end as the last record must be
    // followed by a 0 (length) to signal end of record
    if (recordSize(encodedData) + 2 > blockSize)
      throw new Error('data larger than block size')

    if (nextWriteBlockOffset + recordSize(encodedData) + 2 > blockSize) {
      // doesn't fit
      const nextBlockBuf = Buffer.alloc(blockSize)
      latestBlockBuf = nextBlockBuf
      latestBlockIndex += 1
      nextWriteBlockOffset = 0
      debug("data doesn't fit current block, creating new")
    }

    appendRecord(latestBlockBuf, encodedData, nextWriteBlockOffset)
    cache.set(latestBlockIndex, latestBlockBuf) // update cache
    const fileOffset = nextWriteBlockOffset + latestBlockIndex * blockSize
    nextWriteBlockOffset += recordSize(encodedData)
    blocksToBeWritten.set(latestBlockIndex, {
      blockBuf: latestBlockBuf,
      fileOffset,
    })
    scheduleWrite()
    debug('data inserted at offset %d', fileOffset)
    return fileOffset
  }

  function append(data, cb) {
    if (Array.isArray(data)) {
      let fileOffset = 0
      for (let i = 0, length = data.length; i < length; ++i)
        fileOffset = appendSingle(data[i])

      cb(null, fileOffset)
    } else cb(null, appendSingle(data))
  }

  function appendTransaction(dataArray, cb) {
    if (!Array.isArray(dataArray))
      return cb(
        new Error('appendTransaction expects first argument to be an array')
      )

    let size = 0
    const encodedDataArray = dataArray.map((data) => {
      let encodedData = codec.encode(data)
      if (typeof encodedData === 'string')
        encodedData = Buffer.from(encodedData)
      size += recordSize(encodedData)
      return encodedData
    })

    // we always leave 2 bytes at the end as the last record must be
    // followed by a 0 (length) to signal end of record
    size += 2

    if (size > blockSize) return cb(new Error('data larger than block size'))

    if (nextWriteBlockOffset + size > blockSize) {
      // doesn't fit
      const nextBlockBuf = Buffer.alloc(blockSize)
      latestBlockBuf = nextBlockBuf
      latestBlockIndex += 1
      nextWriteBlockOffset = 0
      debug("data doesn't fit current block, creating new")
    }

    const fileOffsets = []
    encodedDataArray.forEach((encodedData) => {
      appendRecord(latestBlockBuf, encodedData, nextWriteBlockOffset)
      cache.set(latestBlockIndex, latestBlockBuf) // update cache
      const fileOffset = nextWriteBlockOffset + latestBlockIndex * blockSize
      fileOffsets.push(fileOffset)
      nextWriteBlockOffset += recordSize(encodedData)
      blocksToBeWritten.set(latestBlockIndex, {
        blockBuf: latestBlockBuf,
        fileOffset,
      })
      debug('data inserted at offset %d', fileOffset)
    })

    scheduleWrite()

    return cb(null, fileOffsets)
  }

  const scheduleWrite = debounce(write, writeTimeout)

  function writeBlock(blockIndex) {
    if (!blocksToBeWritten.has(blockIndex)) return
    writingBlockIndex = blockIndex
    const { blockBuf, fileOffset } = blocksToBeWritten.get(blockIndex)
    blocksToBeWritten.delete(blockIndex)

    debug(
      'writing block of size: %d, to offset: %d',
      blockBuf.length,
      blockIndex * blockSize
    )
    writeWithFSync(blockIndex * blockSize, blockBuf, null, (err) => {
      const drainsBefore = (waitingDrain.get(blockIndex) || []).slice(0)
      writingBlockIndex = -1
      if (err) {
        debug('failed to write block %d', blockIndex)
        throw err
      } else {
        since.set(fileOffset)

        // write values to live streams
        self.streams.forEach((stream) => {
          if (stream.live) stream.liveResume()
        })

        debug(
          'draining the waiting queue for %d, items: %d',
          blockIndex,
          drainsBefore.length
        )
        for (let i = 0; i < drainsBefore.length; ++i) drainsBefore[i]()

        // the resumed streams might have added more to waiting
        let drainsAfter = waitingDrain.get(blockIndex) || []
        if (drainsBefore.length === drainsAfter.length)
          waitingDrain.delete(blockIndex)
        else if (drainsAfter.length === 0) waitingDrain.delete(blockIndex)
        else
          waitingDrain.set(
            blockIndex,
            waitingDrain.get(blockIndex).slice(drainsBefore.length)
          )

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

  function onLoad(fn) {
    return function (arg, cb) {
      if (latestBlockBuf === null)
        waiting.push(function () {
          fn(arg, cb)
        })
      else fn(arg, cb)
    }
  }

  function onReady(fn) {
    if (latestBlockBuf !== null) fn()
    else waiting.push(fn)
  }

  function onDrain(fn) {
    if (blocksToBeWritten.size === 0 && writingBlockIndex === -1) fn()
    else {
      const latestBlockIndex =
        blocksToBeWritten.size > 0
          ? last(blocksToBeWritten.keys())
          : writingBlockIndex
      const drains = waitingDrain.get(latestBlockIndex) || []
      drains.push(fn)
      waitingDrain.set(latestBlockIndex, drains)
    }
  }

  function last(iterable) {
    let res = null
    for (let x of iterable) res = x
    return res
  }

  return (self = {
    // Public API:
    get: onLoad(get),
    del: onLoad(del),
    append: onLoad(append),
    appendTransaction: onLoad(appendTransaction),
    close: onLoad(close),
    onDrain: onLoad(onDrain),
    since,
    stream(opts) {
      const stream = new Stream(self, opts)
      self.streams.push(stream)
      return stream
    },

    // Internals:
    filename,
    // Internals needed for ./stream.js:
    onReady,
    getNextBlockIndex,
    getDataNextOffset,
    getBlock,
    streams: [],
  })
}
