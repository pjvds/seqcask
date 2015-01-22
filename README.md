# Seqcask

Sequential value store based on the famous [bitcask](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf). 
Seqcask has the following differences compared with bitcask;

* holds to ~35% more keys in RAM
* sequential keys instead of variable
* only support `put` and `get`, no `update` or `delete`
* does not support `buckets`

Of course Seqcask has the same strengths

* single seek to retrieve any value!
* predictable performance
* easy backup, no need to shutdown
* low latency and high throughput
* data integrity

And seqcask shares the same weakness as bitcask:

* all keys need to fit in RAM, 17 bytes per key

# Status
Currently I am focussing on the writes. On my Macbook I am doing over 2.12 million 200 byte sized messages per second. That is 0.415gb per second.

This included hashing, but lacks some meta information. Next is closing the read roundtrip.
