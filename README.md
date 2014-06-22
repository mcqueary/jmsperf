# jmsperf

JMS Performance Test Tools

## Connections and threads

Connections are broker connections. Threads represent individual producers or consumers.

The initialization sequence is as follows:

1. Initialize from property file (if supplied), overridden by command line arguments (if supplied). 
2. Connect to JNDI using provided values
3. Lookup the ConnectionFactory 
4. Create and start (#connections) connections
5. Create and start (#threads) producer/consumer threads

For consumer, each thread grabs a connection from the vector of Connections. This is accomplished by cycling through the connection list in circular fashion, therefore:

* If #threads == #connections, then each thread has a dedicated connection. 
* If #threads > #connections, then some threads are going to be sharing connections (do the math!).
* If #threads < #connections, then some connections will be unused. (This is somewhat pointless on the consumer side. I may force #connections to be equal to #threads in this case. Hopefully I'll also remember to update the README :-))

## Destinations

If unique destinations are specified, then each Producer (or Consumer) thread will produce to (or consume from) the destination name provided, appended with ".1", ".2", ".3", etc, up to the total number of producer/consumer threads. This is useful for measuring unique consumer/destination performance with varying consumer loads. 

If you don't specify unique destinations, then all Producer (or Consumer) threads will produce to (or consume from) the same destination. This is useful for measuring high fan-in or high fan-out performance on a single-destination basis.

## The Test

### Test Duration

Two factors control how long the producer or consumer program will run: "-count" (number of messages) and "-time" (duration in seconds). The test will run until the duration is reached, or the message count is reached, whichever comes first. 

If a duration was specified, that duration will be strictly adhered to, regardless of whether all messages (specified by "-count") get sent or received. If no duration is specified, or if the duration is sufficiently long, then each producer/consumer will produce/consume until all messages have been consumed.

Note that the "count" is the total number of messages that will be produced/consumed by that test instance, irrespective of how many producers or consumers that you create. Each thread will produce/consume roughly count/#threads messages. Therefore, if you wish to certain that EVERY producer/consumer gets some action, you must ensure that count is a multiple of the #threads.

### Special considerations:

* The producer also has a message rate setting. The default is to send as fast as possible, which may cause very jittery and unnatural performance. If rate is set, the producer will do its best to send at the rate specified.

