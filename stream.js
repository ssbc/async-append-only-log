// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const ltgt = require('ltgt')
const looper = require('looper')

module.exports = Stream

function Stream (blocks, opts) {
  opts = opts || {}

  this.blocks = blocks
  this.live = !!opts.live
  this.offsets = opts.offsets !== false
  this.values = opts.values !== false
  this.limit = opts.limit || 0

  this.min = this.max = this.min_inclusive = this.max_inclusive = null
  this.cursor = -1
  this.count = 0
  this.hasWritten = false
  this.writing = false
  this.ended = false
  this.skipNext = false

  this.opts = opts
  this._resumeCallback = this._resumeCallback.bind(this)
  this._resume = this._resume.bind(this)
  this.blocks.onReady(this._ready.bind(this))
}

Stream.prototype._ready = function () {
  this.min = ltgt.lowerBound(this.opts, null)
  if (ltgt.lowerBoundInclusive(this.opts))
    this.min_inclusive = this.min

  this.max = ltgt.upperBound(this.opts, null)
  if (ltgt.upperBoundInclusive(this.opts))
    this.max_inclusive = this.max

  //note: cursor has default of the current length or zero.
  this.cursor = ltgt.lowerBound(this.opts, 0)

  if (this.cursor < 0) this.cursor = 0

  if (this.opts.gt >= 0) this.skipNext = true

  if (!this.live && this.cursor === 0 && this.blocks.since.value === -1)
    this.ended = true

  if (this.live && this.cursor === 0 && this.blocks.since.value === -1)
    this.cursor = -1

  this.resume()
}

Stream.prototype._writeToSink = function (data) {
  if (!this.hasWritten) this.hasWritten = true
  if (this.values) {
    if (this.offsets) this.sink.write({ offset: this.cursor, value: data })
    else this.sink.write(data)
  }
  else
    this.sink.write(this.cursor)
}

// returns true="next block", false="end stream", null="pause"
Stream.prototype._handleBlock = function(block) {
  while (true) {
    if (this.sink.paused) return null
    const [offset, data] = this.blocks.getDataNextOffset(block, this.cursor)

    if (this.skipNext) {
      this.skipNext = false

      if (offset > 0) {
        this.cursor = offset
        continue
      } else if (offset === 0) {
        return true // get next block
      } else if (offset === -1) {
        if (this.live === true)
          this.writing = false
        return false
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
        return true // get next block
      } else if (offset === -1) {
        if (this.live === true)
          this.writing = false
        return false
      }

      if (this.limit > 0 && this.count >= this.limit)
        return false
    } else
      return false
  }
}

Stream.prototype._resume = function () {
  if (!this.sink || this.sink.paused) {
    if (!this.live) this.writing = false
    return
  }

  if (this.ended) {
    if (!this.sink.ended) {
      if (this.ended === true && !this.live) return this.abort()
      else if (this.sink.end)
        return this.sink.end(this.ended === true ? null : this.ended)
    }
    return
  }

  if (this.cursor === -1)
    return // not ready yet

  if (this.live && !this.writing && this.hasWritten)
    return // wait for data

  this.writing = true
  this.blocks.getBlock(this.cursor, this._resumeCallback)
}

Stream.prototype._resumeCallback = function (err, block) {
  if (err) {
    console.error(err)
    return
  }

  const handled = this._handleBlock(block)
  if (handled === true) {
    this.cursor = this.blocks.getNextBlockIndex(this.cursor)
    this._next()
  }
  else if (handled === null) {
    if (!this.live) this.writing = false
    return
  }
  else if (this.live !== true) this.abort()
}

Stream.prototype.resume = function () {
  if (!this.live && this.writing) return
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
