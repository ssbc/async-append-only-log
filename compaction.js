// SPDX-FileCopyrightText: 2022 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const debug = require('debug')('async-append-only-log')
const Record = require('./record')

module.exports = class Compaction {
  constructor(log, lastBlockIndex, opts, onDone) {
    // TODO opts?
    this.log = log
    this.onDone = onDone
    this.LAST_BLOCK_INDEX = lastBlockIndex

    this.compactedBlockIndex = -1
    this.compactedBlockBuf = null
    this.compactedOffset = 0
    this.compactedBlockIdenticalToUnshifted = true

    this.unshiftedBlockIndex = 0
    this.unshiftedBlockBuf = null
    this.unshiftedOffset = 0

    this.compactNextBlock()
  }

  continueCompactingBlock() {
    while (true) {
      // Fetch the unshifted block, if necessary
      if (!this.unshiftedBlockBuf) {
        this.loadUnshiftedBlock(() => {
          this.continueCompactingBlock()
        })
        return
      }
      // When all records have been shifted (thus end of log), stop compacting
      if (this.unshiftedBlockIndex === -1) {
        this.saveCompactedBlock((err) => {
          if (err) throw err
          this.stop()
        })
        return
      }
      const [unshiftedDataBuf, unshiftedRecSize] = this.getUnshiftedRecord()
      // Get a non-deleted unshifted record, if necessary
      if (!unshiftedDataBuf) {
        this.goToNextUnshifted()
        continue
      }
      const compactedBlockStart = this.compactedBlockIndex * this.log.blockSize
      const offsetInCompactedBlock = this.compactedOffset - compactedBlockStart
      // Proceed to compact the next block if this block is full
      if (this.log.hasNoSpaceFor(unshiftedDataBuf, offsetInCompactedBlock)) {
        this.saveCompactedBlock()
        setImmediate(() => this.compactNextBlock())
        return
      }

      if (
        this.compactedBlockIndex !== this.unshiftedBlockIndex ||
        this.compactedOffset !== this.unshiftedOffset
      ) {
        this.compactedBlockIdenticalToUnshifted = false
      }

      // Copy record to new compacted block
      Record.write(
        this.compactedBlockBuf,
        offsetInCompactedBlock,
        unshiftedDataBuf
      )
      this.goToNextUnshifted()
      this.compactedOffset += unshiftedRecSize
    }
  }

  saveCompactedBlock(cb) {
    if (this.compactedBlockIdenticalToUnshifted) {
      if (cb) cb()
    } else {
      const blockIndex = this.compactedBlockIndex
      this.log.overwrite(blockIndex, this.compactedBlockBuf, (err) => {
        if (err && cb) cb(err)
        else if (err) throw err
        else {
          debug('compacted block %d', blockIndex)
          if (cb) cb()
        }
      })
    }
  }

  loadUnshiftedBlock(cb) {
    const blockStart = this.unshiftedBlockIndex * this.log.blockSize
    this.log.getBlock(blockStart, (err, blockBuf) => {
      if (err) return this.onDone(err)
      this.unshiftedBlockBuf = blockBuf
      cb()
    })
  }

  getUnshiftedRecord() {
    const [, dataBuf, recSize] = this.log.getDataNextOffset(
      this.unshiftedBlockBuf,
      this.unshiftedOffset,
      true
    )
    return [dataBuf, recSize]
  }

  goToNextUnshifted() {
    let [nextOffset] = this.log.getDataNextOffset(
      this.unshiftedBlockBuf,
      this.unshiftedOffset,
      true
    )
    if (nextOffset === -1) {
      this.unshiftedBlockIndex = -1
    } else if (nextOffset === 0) {
      this.unshiftedBlockIndex += 1
      this.unshiftedBlockBuf = null
      this.unshiftedOffset = this.unshiftedBlockIndex * this.log.blockSize
    } else {
      this.unshiftedOffset = nextOffset
    }
  }

  compactNextBlock() {
    this.compactedBlockIndex += 1

    const blockStart = this.compactedBlockIndex * this.log.blockSize
    this.compactedBlockBuf = Buffer.alloc(this.log.blockSize)
    this.compactedBlockIdenticalToUnshifted = true
    this.compactedOffset = blockStart
    this.continueCompactingBlock()
  }

  stop() {
    this.compactedBlockBuf = null
    this.unshiftedBlockBuf = null
    this.log.truncate(this.compactedBlockIndex, this.onDone)
  }
}
