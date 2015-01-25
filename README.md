# Seqcask

[![Codeship Status for pjvds/seqcask](https://codeship.com/projects/c6e870a0-8559-0132-5e89-5e1a40b85bb2/status?branch=master)](https://codeship.com/projects/58713)

Value value store optimized for sequential immutible data based on the famous [bitcask](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf). 
Seqcask has the following differences compared with bitcask;

* holds to ~35% more keys in RAM
* sequential keys instead of variable
* only support `put` and `get`, no `update` or `delete`
* does not support `buckets`

Of course Seqcask shares the same *strengths* as bitcask:

* single seek to retrieve any value!
* predictable performance
* easy backup, no need to shutdown
* low latency and high throughput
* data integrity

And Of course Seqcask shares the same *weakness* as bitcask:

* all keys need to fit in RAM (17 bytes of memory per key)

# Status
Currently I am focussing on the writes. On my Macbook I am doing over 2.12 million 200 byte sized messages per second. That is 0.415gb per second.

This included hashing, but lacks some meta information. Next is closing the read roundtrip.

# But why?

Why not? When playing with different storage engines like leveldb and bitcask I got inspired to try to beat the numbers and figured that I should at least learn something while doing so.
