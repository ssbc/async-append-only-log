// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const Cache = require('hashlru')
const RAF = require('polyraf')
const Obv = require('obz')
const debounce = require('lodash.debounce')
const isZeroBuf = require('is-zero-buffer')
const debug = require('debug')('async-append-only-log')
const fs = require('fs')
const mutexify = require('mutexify')

const Stream = require('./stream')
const Record = require('./record')

/**
 * The "End of Block" is a special field used to mark the end of a block, and
 * in practice it's like a Record header "length" field, with the value 0.
 * In most cases, the end region of a block will have a larger length than this,
 * but we want to guarantee there is at *least* this many bytes at the end.
 */
const EOB = {
  SIZE: Record.HEADER_SIZE,
  asNumber: 0,
}

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

  const waitingLoad = []
  const waitingDrain = new Map() // blockIndex -> []
  const blocksToBeWritten = new Map() // blockIndex -> { blockBuf, offset }
  let writingBlockIndex = -1

  let latestBlockBuf = null
  let latestBlockIndex = null
  let nextOffsetInBlock = null
  const since = Obv() // offset of last written record

  raf.stat(function (err, stat) {
    if (err) debug('failed to stat ' + filename, err)

    const fileSize = stat ? stat.size : -1

    if (fileSize <= 0) {
      debug('empty file')
      latestBlockBuf = Buffer.alloc(blockSize)
      latestBlockIndex = 0
      nextOffsetInBlock = 0
      cache.set(0, latestBlockBuf)
      since.set(-1)
      while (waitingLoad.length) waitingLoad.shift()()
    } else {
      const blockStart = fileSize - blockSize
      raf.read(blockStart, blockSize, (err, blockBuf) => {
        if (err) throw err

        getLastGoodRecord(blockBuf, blockStart, (err, offsetInBlock) => {
          if (err) throw err

          latestBlockBuf = blockBuf
          latestBlockIndex = fileSize / blockSize - 1
          const recSize = Record.readSize(blockBuf, offsetInBlock)
          nextOffsetInBlock = offsetInBlock + recSize
          since.set(blockStart + offsetInBlock)

          debug('opened file, since: %d', since.value)

          while (waitingLoad.length) waitingLoad.shift()()
        })
      })
    }
  })

  function getOffsetInBlock(offset) {
    return offset % blockSize
  }

  function getBlockStart(offset) {
    return offset - getOffsetInBlock(offset)
  }

  function getNextBlockStart(offset) {
    return getBlockStart(offset) + blockSize
  }

  function getBlockIndex(offset) {
    return getBlockStart(offset) / blockSize
  }

  const writeLock = mutexify()

  function writeWithFSync(blockStart, blockBuf, successValue, cb) {
    writeLock((unlock) => {
      raf.write(blockStart, blockBuf, (err) => {
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

  function fixBlock(blockBuf, badOffsetInBlock, blockStart, successValue, cb) {
    debug('found invalid record at %d, fixing last block', badOffsetInBlock)
    blockBuf.fill(0, badOffsetInBlock, blockSize)
    writeWithFSync(blockStart, blockBuf, successValue, cb)
  }

  function getLastGoodRecord(blockBuf, blockStart, cb) {
    let lastGoodOffset = 0
    for (let offsetInRecord = 0; offsetInRecord < blockSize; ) {
      const length = Record.readDataLength(blockBuf, offsetInRecord)
      if (length === EOB.asNumber) break
      else {
        const [dataBuf, recSize] = Record.read(blockBuf, offsetInRecord)
        if (offsetInRecord + recSize > blockSize) {
          // corrupt length data
          fixBlock(blockBuf, offsetInRecord, blockStart, lastGoodOffset, cb)
          return
        } else {
          if (validateRecord(dataBuf)) {
            lastGoodOffset = offsetInRecord
            offsetInRecord += recSize
          } else {
            // corrupt message data
            fixBlock(blockBuf, offsetInRecord, blockStart, lastGoodOffset, cb)
            return
          }
        }
      }
    }

    cb(null, lastGoodOffset)
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

  function getData(blockBuf, offsetInBlock, cb) {
    const [dataBuf] = Record.read(blockBuf, offsetInBlock)

    if (isZeroBuf(dataBuf)) {
      const err = new Error('item has been deleted')
      err.code = 'ERR_AAOL_DELETED_RECORD'
      return cb(err)
    } else cb(null, codec.decode(dataBuf))
  }

  function get(offset, cb) {
    if (typeof offset !== 'number' || isNaN(offset))
      return cb(`Offset ${offset} is not a number`)
    else if (offset < 0) return cb(`Offset is ${offset} must be >= 0`)

    getBlock(offset, (err, blockBuf) => {
      if (err) return cb(err)
      getData(blockBuf, getOffsetInBlock(offset), cb)
    })
  }

  // nextOffset can take 3 values:
  // -1: end of log
  //  0: need a new block
  // >0: next record within block
  function getDataNextOffset(blockBuf, offset) {
    const offsetInBlock = getOffsetInBlock(offset)
    const [dataBuf, recSize] = Record.read(blockBuf, offsetInBlock)
    const nextLength = Record.readDataLength(blockBuf, offsetInBlock + recSize)

    let nextOffset
    if (nextLength === EOB.asNumber) {
      if (getNextBlockStart(offset) > since.value) nextOffset = -1
      else nextOffset = 0
    } else {
      nextOffset = offset + recSize
    }

    if (isZeroBuf(dataBuf)) return [nextOffset, null]
    else return [nextOffset, codec.decode(dataBuf)]
  }

  function del(offset, cb) {
    getBlock(offset, (err, blockBuf) => {
      if (err) return cb(err)
      Record.overwriteWithZeroes(blockBuf, getOffsetInBlock(offset))
      // we write directly here to make normal write simpler
      const blockStart = getBlockStart(offset)
      writeWithFSync(blockStart, blockBuf, null, cb)
    })
  }

  function appendSingle(data) {
    let encodedData = codec.encode(data)
    if (typeof encodedData === 'string') encodedData = Buffer.from(encodedData)

    if (Record.size(encodedData) + EOB.SIZE > blockSize)
      throw new Error('data larger than block size')

    if (nextOffsetInBlock + Record.size(encodedData) + EOB.SIZE > blockSize) {
      // doesn't fit
      const nextBlockBuf = Buffer.alloc(blockSize)
      latestBlockBuf = nextBlockBuf
      latestBlockIndex += 1
      nextOffsetInBlock = 0
      debug("data doesn't fit current block, creating new")
    }

    Record.write(latestBlockBuf, nextOffsetInBlock, encodedData)
    cache.set(latestBlockIndex, latestBlockBuf) // update cache
    const offset = latestBlockIndex * blockSize + nextOffsetInBlock
    blocksToBeWritten.set(latestBlockIndex, {
      blockBuf: latestBlockBuf,
      offset,
    })
    nextOffsetInBlock += Record.size(encodedData)
    scheduleWrite()
    debug('data inserted at offset %d', offset)
    return offset
  }

  function append(data, cb) {
    if (Array.isArray(data)) {
      let offset = 0
      for (let i = 0, length = data.length; i < length; ++i)
        offset = appendSingle(data[i])

      cb(null, offset)
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
      size += Record.size(encodedData)
      return encodedData
    })

    size += EOB.SIZE

    if (size > blockSize) return cb(new Error('data larger than block size'))

    if (nextOffsetInBlock + size > blockSize) {
      // doesn't fit
      const nextBlockBuf = Buffer.alloc(blockSize)
      latestBlockBuf = nextBlockBuf
      latestBlockIndex += 1
      nextOffsetInBlock = 0
      debug("data doesn't fit current block, creating new")
    }

    const offsets = []
    for (const encodedData of encodedDataArray) {
      Record.write(latestBlockBuf, nextOffsetInBlock, encodedData)
      cache.set(latestBlockIndex, latestBlockBuf) // update cache
      const offset = latestBlockIndex * blockSize + nextOffsetInBlock
      offsets.push(offset)
      blocksToBeWritten.set(latestBlockIndex, {
        blockBuf: latestBlockBuf,
        offset,
      })
      nextOffsetInBlock += Record.size(encodedData)
      debug('data inserted at offset %d', offset)
    }

    scheduleWrite()

    return cb(null, offsets)
  }

  const scheduleWrite = debounce(write, writeTimeout)

  function writeBlock(blockIndex) {
    if (!blocksToBeWritten.has(blockIndex)) return
    writingBlockIndex = blockIndex
    const { blockBuf, offset } = blocksToBeWritten.get(blockIndex)
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
        since.set(offset)

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
        self.streams.shift().abort(new Error('async-append-only-log closed'))
      raf.close(cb)
    })
  }

  function onLoad(fn) {
    return function waitForLogLoaded(...args) {
      if (latestBlockBuf === null) waitingLoad.push(fn.bind(null, ...args))
      else fn(...args)
    }
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
    onLoad,
    getNextBlockStart,
    getDataNextOffset,
    getBlock,
    streams: [],
  })
}
