var ltgt = require('ltgt')
module.exports = Stream

function Stream (blocks, opts) {
  opts = opts || {}
  this.live = !!opts.live
  this.blocks = blocks
  this.cursor = -1
  this.seqs = opts.seqs !== false
  this.values = opts.values !== false
  this.limit = opts.limit || 0
  this.count = 0
  this.min = this.max = this.min_inclusive = this.max_inclusive = null

  var self = this
  this.opts = opts
  this.blocks.onReady(this._ready.bind(this))
}

Stream.prototype._ready = function () {
  this.min = ltgt.lowerBound(this.opts, null)
  if(ltgt.lowerBoundInclusive(this.opts))
    this.min_inclusive = this.min

  this.max = ltgt.upperBound(this.opts, null)
  if(ltgt.upperBoundInclusive(this.opts))
    this.max_inclusive = this.max

  //note: cursor has default of the current length or zero.
  this.cursor = ltgt.lowerBound(this.opts, 0)

  if(this.cursor < 0) this.cursor = 0

  if(!this.live && this.cursor === 0 && this.blocks.length == 0) {
    this.ended = true
    return this.resume()
  } else
    this.resume()
}

Stream.prototype._writeToSink = function (data) {
  if (this.values) {
    if (!data.every(x => x === 0)) // skip deleted
    {
      if (this.seqs) this.sink.write({ seq: this.cursor, value: data })
      else this.sink.write(data)
    }
  }
  else
    this.sink.write(this.cursor)
}

Stream.prototype._handleResult = function(err, result) {
  var o = this.cursor
  this.count++
  if(
    (this.min === null || this.min < o || this.min_inclusive === o) &&
    (this.max === null || this.max > o || this.max_inclusive === o)
  ) {
    this._writeToSink(result[1])
    if (this.live === true) {
      if (result[0] != -1)
        this.cursor = result[0]
      else
        return
    }
    else
      this.cursor = result[0]

    if (this.sink && !this.sink.paused && this.cursor != -1)
      this.blocks.getNext(this.cursor, this._handleResult.bind(this))
    else if (this.live !== true)
      this.abort()
  }
  else {
    if(this.limit > 0 && this.count >= this.limit) {
      this.abort()
      this.sink.end()
    }
  }
}

Stream.prototype.resume = function () {
  if(!this.sink || this.sink.paused) return

  if(this.ended && !this.sink.ended)
    return this.sink.end(this.ended === true ? null : this.ended)

  if (this.cursor === -1)
    return // not ready yet

  this.blocks.getNext(this.cursor, this._handleResult.bind(this))
}

Stream.prototype.abort = function (err) {
  //only thing to do is unsubscribe from live stream.
  //but append isn't implemented yet...
  this.ended = err || true
  var i = this.blocks.streams.indexOf(this)
  if(~i) this.blocks.streams.splice(i, 1)
  if(!this.sink.ended)
    this.sink.end(err === true ? null : err)
}

Stream.prototype.pipe = require('push-stream/pipe')
