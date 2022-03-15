// SPDX-FileCopyrightText: 2022 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const RAF = require('polyraf')
const fs = require('fs')
const mutexify = require('mutexify')
const debug = require('debug')('async-append-only-log')
const Record = require('./record')

/**
 * This file has state describing the continuation of the compaction algorithm.
 *
 * - bytes 0 and 1: UInt32LE for the blockIndex to-be-compacted
 * - bytes 1 and 2: UInt32LE for the 1st unshifted record's offset
 * - bytes 3 until 3+blockSize: blockBuf containing the 1st unshifted record
 */
class PersistentState {
  constructor(logFilename, blockSize) {
    this.raf = RAF(PersistentState.filename(logFilename))
    this.blockSize = blockSize
    this.writeLock = mutexify()
  }

  static filename(logFilename) {
    return logFilename + '.compaction'
  }

  static exists(logFilename) {
    return fs.existsSync(PersistentState.filename(logFilename))
  }

  load(cb) {
    this.raf.stat((err, stat) => {
      const fileSize = !err && stat ? stat.size : -1
      if (fileSize <= 0) {
        const state = {
          compactedBlockIndex: 0,
          unshiftedOffset: 0,
          unshiftedBlockBuffer: null,
          initial: true,
        }
        cb(null, state)
      } else {
        const stateFileSize = 2 + 2 + this.blockSize
        this.raf.read(0, stateFileSize, (err, buf) => {
          if (err) return cb(err)
          const state = {
            compactedBlockIndex: buf.readUInt16LE(0),
            unshiftedOffset: buf.readUInt16LE(2) - 1,
            unshiftedBlockBuf: buf.slice(4),
            initial: false,
          }
          cb(null, state)
        })
      }
    })
  }

  save(state, cb) {
    const buf = Buffer.alloc(4)
    buf.writeUInt16LE(state.compactedBlockIndex, 0)
    buf.writeUInt16LE(state.unshiftedOffset + 1, 2) // FIXME: could offset be -1?
    state.unshiftedBlockBuf.copy(buf, 4)
    this.writeLock((unlock) => {
      this.raf.write(0, buf, (err) => {
        if (err) return unlock(cb, err)
        if (this.raf.fd) {
          fs.fsync(this.raf.fd, (err) => {
            if (err) unlock(cb, err)
            else unlock(cb, null, state)
          })
        } else unlock(cb, null, state)
      })
    })
  }

  delete(cb) {
    this.raf.close((err) => {
      if (err) return cb(err)
      fs.unlink(this.raf.filename, (err) => {
        if (err) return cb(err)
        else cb()
      })
    })
  }
}

/**
 * Compaction is the process of removing deleted records from the log by
 * rewriting blocks in the log *in situ*, moving ("shifting") subsequent records
 * to earlier slots, to fill the spaces left by the deleted records.
 *
 * The compaction algorithm is, at a high level:
 * - Keep track of some state, comprised of:
 *   - compactedBlockIndex: blockIndex of the current block being compacted.
 *     all blocks before this have already been compacted. This state always
 *     increases, never decreases.
 *   - unshiftedOffset: offset of the first unshifted record in the log, that
 *     is, the first record that has not been shifted to earlier slots. This
 *     offset is greater or equal to the compacted block's start offset, and
 *     may be either in the same block as the compacted block, or even in a much
 *     later block. This state always increases, never decreases.
 *   - unshiftedBlockBuf: the block containing the first unshifted record
 * - Save the state to disk
 * - Compact one block at a time, in increasing order of blockIndex
 * - When a block is compacted, the state file is updated
 * - Once all blocks have been compacted, delete the state file
 */
class Compaction {
  constructor(log, lastBlockIndex, onDone) {
    this.log = log
    this.persistentState = new PersistentState(log.filename, log.blockSize)
    this.onDone = onDone
    this.LAST_BLOCK_INDEX = lastBlockIndex

    this.compactedBlockIndex = -1
    this.compactedBlockBuf = null
    this.compactedOffset = 0
    this.compactedBlockIdenticalToUnshifted = true

    this.unshiftedBlockIndex = 0
    this.unshiftedBlockBuf = null
    this.unshiftedOffset = 0

    this.loadPersistentState((err) => {
      if (err) return this.onDone(err)
      this.compactedBlockIndex -= 1 // because it'll be incremented very soon
      this.compactNextBlock()
    })
  }

  static stateFileExists(logFilename) {
    return PersistentState.exists(logFilename)
  }

  loadPersistentState(cb) {
    this.persistentState.load((err, state) => {
      if (err) return cb(err)
      this.compactedBlockIndex = state.compactedBlockIndex
      this.unshiftedOffset = state.unshiftedOffset
      this.unshiftedBlockBuf = state.unshiftedBlockBuf
      this.unshiftedBlockIndex = Math.floor(
        state.unshiftedOffset / this.log.blockSize
      )
      if (state.initial) {
        this.savePersistentState(cb)
      } else {
        cb()
      }
    })
  }

  savePersistentState(cb) {
    if (!this.unshiftedBlockBuf) {
      this.loadUnshiftedBlock(() => {
        saveIt.call(this)
      })
    } else {
      saveIt.call(this)
    }

    function saveIt() {
      this.persistentState.save(
        {
          compactedBlockIndex: this.compactedBlockIndex,
          unshiftedOffset: this.unshiftedOffset,
          unshiftedBlockBuf: this.unshiftedBlockBuf,
        },
        cb
      )
    }
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
          if (err) return this.onDone(err)
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
        else if (err) return this.onDone(err)
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

    this.compactedBlockBuf = Buffer.alloc(this.log.blockSize)
    this.compactedOffset = this.compactedBlockIndex * this.log.blockSize
    this.compactedBlockIdenticalToUnshifted = true
    this.savePersistentState((err) => {
      if (err) return this.onDone(err)
      this.continueCompactingBlock()
    })
  }

  stop() {
    this.compactedBlockBuf = null
    this.unshiftedBlockBuf = null
    this.persistentState.delete((err) => {
      if (err) return this.onDone(err)
      this.log.truncate(this.compactedBlockIndex, this.onDone)
    })
  }
}

module.exports = Compaction