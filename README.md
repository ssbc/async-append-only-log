# Async flumelog

Status: very early WIP. Don't use this for anything

This module is heavily inspired by [flumelog-aligned-offset]. It is an
attempt to write something simpler that is easier to reason
about. Flumelog is the lowest part of the SSB stack, so it should
extremly stable while still maintaining good performance.

A DS flumelog consists of a number of blocks, that contain a number of
records. The records are simply a length + data. A record must be in
one and only one block, which means there will be some empty space at
the end of a block. Blocks are always written in full.

```
<block>
  <record
    <record.length: UInt16LE>
    <record.data>
  </record>*
</block>*
```

Contrasting to flumelog-aligned-offset there is no length after the
data in a record and no pointer at the end of a block. These were to
be able to run the log in reverse, but I have never seen the need for
that.

Writing to the log is always async. Note this is different from
[flumelog-offset] and [flumelog-aligned-offfset]. The since observable
will be updated once the data is written. `onDrain` can be used to
know when data has been written if needed. Streaming will only emit
values that have been written to storage. This is to ensure that a
view will never to ahead of the main log and thus end up in a bad
state if the system crashes before data is written. `get` will return
values that have not been written to disk yet.

## Benchmarks

Running [bench-flumelog] reveals the following numbers. Async flumelog
is faster in all tests except random, where flumelog aligned offset is
a lot faster. Append is very slow for flumelog-aligned-offset because
it writes every message synchronously. The most important numbers are
append (used for onboarding) and stream (used for building indexes).

```
async flumelog:

name, ops/second, mb/second, ops, total-mb, seconds
append, 834834.433, 124.705, 4175007, 623.65, 5.001
stream, 1346776.451, 201.177, 4175007, 623.65, 3.1
stream no cache, 997373.865, 148.984, 4175007, 623.65, 4.186
stream10, 2087121.645, 311.767, 10500309, 1568.502, 5.031
random, 31240.951, 4.666, 156236, 23.337, 5.001

flumelog aligned offset:

name, ops/second, mb/second, ops, total-mb, seconds
append, 769.2, 0.114, 3856, 0.576, 5.013
stream, 128533.333, 19.207, 3856, 0.576, 0.03
stream no cache, 124387.096, 18.587, 3856, 0.576, 0.031
stream10, 428444.444, 64.023, 38560, 5.762, 0.09
random, 907996.6, 135.684, 4540891, 678.556, 5.001

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


[flumelog-aligned-offset]: https://github.com/flumedb/flumelog-aligned-offset/
[flumelog-offset]: https://github.com/flumedb/flumelog-offset/
[bench-flumelog]: https://github.com/flumedb/bench-flumelog
