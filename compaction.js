// SPDX-FileCopyrightText: 2022 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const debug = require('debug')('async-flumelog')
const Record = require('./record')

module.exports = class Compaction {
  constructor(log, lastBlockIndex, opts, onDone) {
    // TODO opts?
    this.log = log
    this.onDone = onDone
    this.LAST_BLOCK_INDEX = lastBlockIndex

    this.compactBlockIndex = -1
    this.compactBlockBuf = null
    this.compactOffset = 0
    this.compactBlockIdenticalToUnshifted = true

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
          this.stop(this.compactBlockIndex)
        })
        return
      }
      const [unshiftedDataBuf, unshiftedRecSize] = this.getUnshiftedRecord()
      // Get a non-deleted unshifted record, if necessary
      if (!unshiftedDataBuf) {
        this.goToNextUnshifted()
        continue
      }
      const compactBlockStart = this.compactBlockIndex * this.log.blockSize
      const offsetInCompactBlock = this.compactOffset - compactBlockStart
      // Proceed to compact the next block if this block is full
      if (this.log.hasNoSpaceFor(unshiftedDataBuf, offsetInCompactBlock)) {
        this.saveCompactedBlock()
        setImmediate(() => this.compactNextBlock())
        return
      }

      if (
        this.compactBlockIndex !== this.unshiftedBlockIndex ||
        this.compactOffset !== this.unshiftedOffset
      ) {
        this.compactBlockIdenticalToUnshifted = false
      }

      // Copy record to new compacted block
      Record.write(this.compactBlockBuf, offsetInCompactBlock, unshiftedDataBuf)
      this.goToNextUnshifted()
      this.compactOffset += unshiftedRecSize
    }
  }

  saveCompactedBlock(cb) {
    if (this.compactBlockIdenticalToUnshifted) {
      if (cb) cb()
    } else {
      const blockIndex = this.compactBlockIndex
      this.log.overwrite(blockIndex, this.compactBlockBuf, (err) => {
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
    const lastCompactedBlockIndex = this.compactBlockIndex
    this.compactBlockIndex += 1

    if (
      this.compactBlockIndex > this.LAST_BLOCK_INDEX ||
      this.unshiftedBlockIndex === -1
    ) {
      this.stop(lastCompactedBlockIndex)
      return
    }

    const blockStart = this.compactBlockIndex * this.log.blockSize
    this.compactBlockBuf = Buffer.alloc(this.log.blockSize)
    this.compactBlockIdenticalToUnshifted = true
    this.compactOffset = blockStart
    this.continueCompactingBlock()
  }

  stop(lastBlockIndex) {
    this.compactBlockBuf = null
    this.unshiftedBlockBuf = null
    this.log.truncate(lastBlockIndex, this.onDone)
  }
}
