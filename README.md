# Async append only log

This module is heavily inspired by [flumelog-aligned-offset]. It is an
attempt to implement the same concept but in a simpler fashion, making
it easier to reason about the code. A log is the lowest part of the
SSB stack, so it should extremly stable while still maintaining good
performance.

A log consists of a number of `blocks`, that contain a number of
`record`s. A `record` is simply it's `length`, as a 16-bit unsigned
integer, followed by the `data` bytes. A record must be in one and
only one block, which means there probably will be some empty space at
the end of a block.  Blocks are always written in full.

```
<block>
  <record
    <length: UInt16LE>
    <data: Bytes>
  </record>*
</block>*
```

In contrast to flumelog-aligned-offset there is no additional `length`
after the `data` in a `record` and no pointer at the end of a
`block`. These were there to be able to iterate over the log in
reverse, but I have never seen the need for that.

Writing to the log is always async. Note this is different from
[flumelog-offset] and [flumelog-aligned-offset]. The `since`
observable will be updated once the data is written. The `onDrain`
callback can be used to know when data has been written if
needed. Streaming will only emit values that have been written to
storage. This is to ensure that a view will never get ahead of the
main log and thus end up in a bad state if the system crashes before
data is written. `get` will return values that have not been written
to disk yet.

This module is not compatible with flume without a wrapper around
stream as it uses the same terminology as [JITDB] and [ssb-db2] of
using offset for the byte position of a record instead of seq.

## Options

```
var OffsetLog = require('async-flumelog')
var log = OffsetLog('/data/log', {
  blockSize: 1024,          // default is 1024*64
  codec: {encode, decode}   // defaults to no codec, expects buffers. for json use flumecodec/json
  writeTimeout: 100         // default is 250. Amount of time to wait between writes
  validateRecord: (d) => {} // default is no validate. A custom function that takes a message and
                            // runs a custom validation to ensure the record is valid.
                            // On load, all records in the latest block will be checked using this
})
```

## Benchmarks

Running [bench-flumelog] reveals the following numbers. Async flumelog
is faster that regular flumelog-offset in all categories. The most
important numbers are append (used for onboarding) and stream (used
for building indexes). Flumelog-aligned-offset is not included in the
benchmarks, as it writes every message synchronously rendering the
results invalid.

```
async flumelog:

name, ops/second, mb/second, ops, total-mb, seconds
append, 923964.807, 138.002, 4620748, 690.149, 5.001
stream, 1059075.865, 158.182, 4620748, 690.149, 4.363
stream no cache, 1102803.818, 164.713, 4620748, 690.149, 4.19
stream10, 2540947.641, 379.51, 12714902, 1899.068, 5.004
random, 39715.656, 5.931, 198618, 29.664, 5.001

flumelog offset:

name, ops/second, mb/second, ops, total-mb, seconds
append, 306180.037, 45.74, 3064556, 457.817, 10.009
stream, 294511.348, 43.997, 2945408, 440.017, 10.001
stream no cache, 327724.949, 48.959, 3064556, 457.817, 9.351
stream10, 452973.302, 67.67, 4530186, 676.776, 10.001
random, 28774.712, 4.298, 287891, 43.008, 10.005
```

To run the benchmarks the small `bench-flumelog.patch` needs to be
applied.

[JITDB] results for more real world benchmarks are available as [jitdb-results].

[flumelog-aligned-offset]: https://github.com/flumedb/flumelog-aligned-offset/
[flumelog-offset]: https://github.com/flumedb/flumelog-offset/
[bench-flumelog]: https://github.com/flumedb/bench-flumelog
[JITDB]: https://github.com/ssb-ngi-pointer/jitdb/
[jitdb-results]: https://github.com/arj03/jitdb/blob/master/bench.txt
[ssb-db2]: https://github.com/ssb-ngi-pointer/ssb-db2/
