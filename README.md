# kafka-forwarder

Simple Message forwarder between Kafka clusters.


## Configuration

Cluster and forwarding configuration is written in a simple DSL
(see main_example.py).


## Service Guarantee

This is free software an there are no guarantees at all.

Yet, in principle this service should provide at least once delivery,
but not exactly once.  E.g. aborting the program while sending a
message will resend the message and several before, regardless of
whether they were already successfully delivered.

## Scaling

Multiple instances will share the workload (via Kafka's usual
group_ids).
