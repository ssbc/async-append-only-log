# Dead simple flumelog

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

[flumelog-aligned-offset]: https://github.com/flumedb/flumelog-aligned-offset/
[flumelog-offset]: https://github.com/flumedb/flumelog-offset/
