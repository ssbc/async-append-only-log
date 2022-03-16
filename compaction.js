// SPDX-FileCopyrightText: 2022 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const RAF = require('polyraf')
const fs = require('fs')
const mutexify = require('mutexify')
const debug = require('debug')('async-append-only-log')
const Record = require('./record')

function getStateFilename(logFilename) {
  return logFilename + '.compaction'
}

/**
 * This file has state describing the continuation of the compaction algorithm.
 *
 * - bytes 0..3: UInt32LE for the blockIndex to-be-compacted
 * - bytes 4..7: UInt32LE for the 1st unshifted record's offset
 * - bytes 8..(8+blockSize-1): blockBuf containing the 1st unshifted record
 */
function PersistentState(logFilename, blockSize) {
  const raf = RAF(getStateFilename(logFilename))
  const writeLock = mutexify()

  function load(cb) {
    raf.stat((err, stat) => {
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
        const stateFileSize = 4 + 4 + blockSize
        raf.read(0, stateFileSize, (err, buf) => {
          if (err) return cb(err)
          const state = {
            compactedBlockIndex: buf.readUInt32LE(0),
            unshiftedOffset: buf.readUInt32LE(4),
            unshiftedBlockBuf: buf.slice(8),
            initial: false,
          }
          cb(null, state)
        })
      }
    })
  }

  function save(state, cb) {
    const buf = Buffer.alloc(4 + 4 + blockSize)
    buf.writeUInt32LE(state.compactedBlockIndex, 0)
    buf.writeUInt32LE(state.unshiftedOffset, 4)
    state.unshiftedBlockBuf.copy(buf, 8)
    writeLock((unlock) => {
      raf.write(0, buf, (err) => {
        if (err) return unlock(cb, err)
        if (raf.fd) {
          fs.fsync(raf.fd, (err) => {
            if (err) unlock(cb, err)
            else unlock(cb, null, state)
          })
        } else unlock(cb, null, state)
      })
    })
  }

  function destroy(cb) {
    raf.close((err) => {
      if (err) return cb(err)
      fs.unlink(raf.filename, (err) => {
        if (err) return cb(err)
        else cb()
      })
    })
  }

  return {
    load,
    save,
    destroy,
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
function Compaction(log, onDone) {
  const persistentState = PersistentState(log.filename, log.blockSize)

  let compactedBlockIndex = -1
  let compactedBlockBuf = null
  let compactedOffset = 0
  let compactedBlockIdenticalToUnshifted = true

  let unshiftedBlockIndex = 0
  let unshiftedBlockBuf = null
  let unshiftedOffset = 0

  loadPersistentState((err) => {
    if (err) return onDone(err)
    compactedBlockIndex -= 1 // because it'll be incremented very soon
    compactNextBlock()
  })

  function loadPersistentState(cb) {
    persistentState.load((err, state) => {
      if (err) return cb(err)
      compactedBlockIndex = state.compactedBlockIndex
      unshiftedOffset = state.unshiftedOffset
      unshiftedBlockBuf = state.unshiftedBlockBuf
      unshiftedBlockIndex = Math.floor(state.unshiftedOffset / log.blockSize)
      if (state.initial) {
        savePersistentState(cb)
      } else {
        cb()
      }
    })
  }

  function savePersistentState(cb) {
    if (!unshiftedBlockBuf) {
      loadUnshiftedBlock(() => {
        saveIt.call(this)
      })
    } else {
      saveIt.call(this)
    }

    function saveIt() {
      persistentState.save(
        {
          compactedBlockIndex,
          unshiftedOffset,
          unshiftedBlockBuf,
        },
        cb
      )
    }
  }

  function continueCompactingBlock() {
    while (true) {
      // Fetch the unshifted block, if necessary
      if (!unshiftedBlockBuf) {
        loadUnshiftedBlock(() => {
          continueCompactingBlock()
        })
        return
      }
      // When all records have been shifted (thus end of log), stop compacting
      if (unshiftedBlockIndex === -1) {
        saveCompactedBlock((err) => {
          if (err) return onDone(err)
          stop()
        })
        return
      }
      const [unshiftedDataBuf, unshiftedRecSize] = getUnshiftedRecord()
      // Get a non-deleted unshifted record, if necessary
      if (!unshiftedDataBuf) {
        goToNextUnshifted()
        continue
      }
      const compactedBlockStart = compactedBlockIndex * log.blockSize
      const offsetInCompactedBlock = compactedOffset - compactedBlockStart
      // Proceed to compact the next block if this block is full
      if (log.hasNoSpaceFor(unshiftedDataBuf, offsetInCompactedBlock)) {
        saveCompactedBlock()
        setImmediate(() => compactNextBlock())
        return
      }

      if (
        compactedBlockIndex !== unshiftedBlockIndex ||
        compactedOffset !== unshiftedOffset
      ) {
        compactedBlockIdenticalToUnshifted = false
      }

      // Copy record to new compacted block
      Record.write(compactedBlockBuf, offsetInCompactedBlock, unshiftedDataBuf)
      goToNextUnshifted()
      compactedOffset += unshiftedRecSize
    }
  }

  function saveCompactedBlock(cb) {
    if (compactedBlockIdenticalToUnshifted) {
      if (cb) cb()
    } else {
      const blockIndex = compactedBlockIndex
      log.overwrite(blockIndex, compactedBlockBuf, (err) => {
        if (err && cb) cb(err)
        else if (err) return onDone(err)
        else {
          debug('compacted block %d', blockIndex)
          if (cb) cb()
        }
      })
    }
  }

  function loadUnshiftedBlock(cb) {
    const blockStart = unshiftedBlockIndex * log.blockSize
    log.getBlock(blockStart, (err, blockBuf) => {
      if (err) return onDone(err)
      unshiftedBlockBuf = blockBuf
      cb()
    })
  }

  function getUnshiftedRecord() {
    const [, dataBuf, recSize] = log.getDataNextOffset(
      unshiftedBlockBuf,
      unshiftedOffset,
      true
    )
    return [dataBuf, recSize]
  }

  function goToNextUnshifted() {
    let [nextOffset] = log.getDataNextOffset(
      unshiftedBlockBuf,
      unshiftedOffset,
      true
    )
    if (nextOffset === -1) {
      unshiftedBlockIndex = -1
    } else if (nextOffset === 0) {
      unshiftedBlockIndex += 1
      unshiftedBlockBuf = null
      unshiftedOffset = unshiftedBlockIndex * log.blockSize
    } else {
      unshiftedOffset = nextOffset
    }
  }

  function compactNextBlock() {
    compactedBlockIndex += 1

    compactedBlockBuf = Buffer.alloc(log.blockSize)
    compactedOffset = compactedBlockIndex * log.blockSize
    compactedBlockIdenticalToUnshifted = true
    savePersistentState((err) => {
      if (err) return onDone(err)
      continueCompactingBlock()
    })
  }

  function stop() {
    compactedBlockBuf = null
    unshiftedBlockBuf = null
    persistentState.destroy((err) => {
      if (err) return onDone(err)
      log.truncate(compactedBlockIndex, onDone)
    })
  }

  return {}
}

Compaction.stateFileExists = function stateFileExists(logFilename) {
  return fs.existsSync(getStateFilename(logFilename))
}

module.exports = Compaction
