# Async flumelog

This module is heavily inspired by [flumelog-aligned-offset]. It is an
attempt to implement the same concept but in a simpler fashion,
making it easier to reason about the code.
Flumelog is the lowest part of the SSB stack, so it should
extremly stable while still maintaining good performance.

An async flumelog consists of a number of `blocks`, that contain a
number of `record`s. A `record` is simply it's `length`, as a 16-bit unsigned integer,
followed by the `data` bytes. A record must be in one and only one block,
which means there probably will be some empty space at the end of a block.
Blocks are always written in full.

```
<block>
  <record
    <length: UInt16LE>
    <data: Bytes>
  </record>*
</block>*
```

In contrast to flumelog-aligned-offset there is no additional `length` after the
`data` in a `record` and no pointer at the end of a `block`. These were there to
be able to iterate over the log in reverse, but I have never seen the need for
that.

Writing to the log is always async. Note this is different from
[flumelog-offset] and [flumelog-aligned-offfset]. The `since` observable
will be updated once the data is written. The `onDrain` callback can be used to
know when data has been written if needed. Streaming will only emit
values that have been written to storage. This is to ensure that a
view will never get ahead of the main log and thus end up in a bad
state if the system crashes before data is written. `get` will return
values that have not been written to disk yet.

## Benchmarks

Running [bench-flumelog] reveals the following numbers. Async flumelog
is faster in all tests except random, where flumelog aligned offset is
a lot faster. The `append` test is very slow for flumelog-aligned-offset
because it writes every message synchronously. The most important numbers 
are append (used for onboarding) and stream (used for building indexes).

```
async flumelog:

name, ops/second, mb/second, ops, total-mb, seconds
append, 672175.964, 100.417, 3361552, 502.188, 5.001
stream, 1276215.641, 190.656, 3361552, 502.188, 2.634
stream no cache, 1355464.516, 202.495, 3361552, 502.188, 2.48
stream10, 2046797.202, 305.777, 10244220, 1530.418, 5.005
random, 21936.533, 3.277, 110604, 16.522, 5.042

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

[JITDB] results for more real world benchmarks are available as [jitdb-results].

[flumelog-aligned-offset]: https://github.com/flumedb/flumelog-aligned-offset/
[flumelog-offset]: https://github.com/flumedb/flumelog-offset/
[bench-flumelog]: https://github.com/flumedb/bench-flumelog
[JITDB]: https://github.com/arj03/jitdb/
[jitdb-results]: https://github.com/arj03/jitdb/blob/master/bench.txt
