// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const ltgt = require('ltgt')
const looper = require('looper')

module.exports = Stream

BLOCK_STATE = Object.freeze({
  GET_NEXT_BLOCK: 0,
  END_OF_STREAM: 1,
  PAUSED: 2
})

function Stream (blocks, opts) {
  opts = opts || {}

  this.blocks = blocks

  // configs
  this.offsets = opts.offsets !== false
  this.values = opts.values !== false
  this.limit = opts.limit || 0

  // state machine
  this.writing = false // resume idempotent
  this.ended = false // abort
  this.skipNext = false // opts.gt

  this.min = ltgt.lowerBound(opts, null)
  if (ltgt.lowerBoundInclusive(opts))
    this.min_inclusive = this.min

  this.max = ltgt.upperBound(opts, null)
  if (ltgt.upperBoundInclusive(opts))
    this.max_inclusive = this.max

  // this is properly initialized when this.blocks is ready
  this.cursor = -1

  // used together with limit
  this.count = 0

  // needed in _ready
  this.opts = opts

  this._resumeCallback = this._resumeCallback.bind(this)
  this._resume = this._resume.bind(this)

  this.blocks.onReady(this._ready.bind(this))
}

Stream.prototype._ready = function () {
  //note: cursor has default of the current length or zero.
  this.cursor = ltgt.lowerBound(this.opts, 0)

  if (this.cursor < 0) this.cursor = 0

  if (this.opts.gt >= 0) this.skipNext = true

  if (this.cursor === 0 && this.blocks.since.value === -1)
    this.ended = true

  this.resume()
}

Stream.prototype._writeToSink = function (data) {
  if (this.values) {
    if (this.offsets) this.sink.write({ offset: this.cursor, value: data })
    else this.sink.write(data)
  }
  else
    this.sink.write(this.cursor)
}

// returns a new BLOCK_STATE
Stream.prototype._handleBlock = function(block) {
  while (true) {
    if (this.sink.paused) return BLOCK_STATE.PAUSED

    const [offset, data] = this.blocks.getDataNextOffset(block, this.cursor)

    if (this.skipNext) {
      this.skipNext = false

      if (offset > 0) {
        this.cursor = offset
        continue
      } else if (offset === 0) {
        return BLOCK_STATE.GET_NEXT_BLOCK
      } else if (offset === -1) {
        return BLOCK_STATE.END_OF_STREAM
      }
    }

    this.count++

    const o = this.cursor

    if (
      (this.min === null || this.min < o || this.min_inclusive === o) &&
      (this.max === null || this.max > o || this.max_inclusive === o)
    ) {
      this._writeToSink(data)

      if (offset > 0)
        this.cursor = offset
      else if (offset === 0) {
        return BLOCK_STATE.GET_NEXT_BLOCK
      } else if (offset === -1) {
        return BLOCK_STATE.END_OF_STREAM
      }

      if (this.limit > 0 && this.count >= this.limit)
        return BLOCK_STATE.END_OF_STREAM
    } else
      return BLOCK_STATE.END_OF_STREAM
  }
}

Stream.prototype._resume = function () {
  if (!this.sink || this.sink.paused) {
    this.writing = false
    return
  }

  if (this.ended) {
    if (!this.sink.ended) {
      if (this.ended === true) return this.abort()
      else if (this.sink.end)
        return this.sink.end(this.ended === true ? null : this.ended)
    }
    return
  }

  if (this.cursor === -1)
    return // not ready yet

  this.writing = true
  this.blocks.getBlock(this.cursor, this._resumeCallback)
}

Stream.prototype._resumeCallback = function (err, block) {
  if (err) {
    console.error(err)
    return
  }

  const blockState = this._handleBlock(block)
  if (blockState === BLOCK_STATE.GET_NEXT_BLOCK) {
    this.cursor = this.blocks.getNextBlockIndex(this.cursor)
    this._next()
  }
  else if (blockState === BLOCK_STATE.PAUSED) {
    this.writing = false
    return
  }
  else this.abort() // END OF STREAM
}

Stream.prototype.resume = function () {
  if (this.writing) return

  this._next = looper(this._resume)
  this._next()
}

Stream.prototype.abort = function (err) {
  this.ended = err || true
  const i = this.blocks.streams.indexOf(this)
  if (~i) this.blocks.streams.splice(i, 1)
  if (!this.sink.ended && this.sink.end) {
    this.sink.ended = true
    this.sink.end(err === true ? null : err)
  }
}

Stream.prototype.pipe = require('push-stream/pipe')
