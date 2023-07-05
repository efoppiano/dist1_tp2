## Error: Missing packets

### Summary

An incorrect was detected in a run.

The error was particularly hard to find, taking 4 days to debug, and stemming from an unexpected source.

Ultimately the error lay in the fact that there was a small chance a packet failed to be sent by pika's `basic_publish` method.

### Analysis

#### Error Detection

Initially, an incorrect result was detected after a long run.

The results were missing some counts (number of rows used to calculate a response) so we deducted that a packet went missing.

#### Initial hypotheses & Course of Action

The observed behavior did not tell us much about the cause of the error.

Our main guesses were that either:

- A packet was being wrongly marked as duplicate.
  - Due to the id generation.
  - Due to the dupe detection.
- The state was being saved incorrectly.

Given that, our main priorities were to:

- Find a way to reproduce the error
- Find ways to better understand the nature of the error

#### Error Reproduction

In order to increase the chance of reproducing the error, we tried:

- Reducing filter thresholds, so that a missing packet would not be hidden by a condition not being met
- Adding a chance of failure after every instruction of _suspected_ failure points
- Increasing the amount of replicas
- Increasing the rate of killing
- Increasing the system load:
  - Using multiple concurrent clients
  - Reducing batch sizes, thus increasing number of packets

These measures did not assure the error would be reproduced, but increased the chance of it happening in a reasonable amount of time.

#### Error Identification

##### Hypothesis testing

To test if packets were **wrongly being marked as dupes** we reduced the maximum sequence number from `512` to `2`.
Given that, as long as a node receives packets with different sequence numbers interleaved it should not mark any as dupes;
then, having only two sequence numbers should increase the chance of errors if the dupe detection was faulty.

> This seemed to increase the amount of errors.

To test if the **state was being saved incorrectly** we tested going back to the original state saver, that saved state in a single step; rather than the current one, that uses a log file and checkpoints.

> This reduced the amount of errors, but did not eliminate them.

##### Traceability of packets

In order to better understand the nature of the error, we added the following mechanisms to detect anomalies:

- Packet hashes: packets were given a hash based on their contents, this not only allowed for a better traceability, but also allowed us to detect if packets were being wrongly marked as dupes (since a dupe should have the same hash as the original).
- May be dupe: Given that we know the sequence of events that can generate duplicate packets, we can know if a packet should not be a dupe by keeping a adding a flag to the packets and nodes.
  > A packet may only be a dupe if:
  >
  > - It is the first packet read after a node restarts
  > - It is a packet generated as a response to the previous case
- Message order: We know sequence numbers should be incremental, except for when they loop back; thus we can detect if a packet is out of order or missing.

#### Missing messages

Eventually, in a run where the error was reproduced, we found the following logs:

```bash
distance_calculator_1  | 17:48:23 - Sending 398-ecee6bb7 to dist_mean_calculator_1
distance_calculator_1  | 17:48:24 - Sending 399-6529392b to dist_mean_calculator_1
dist_mean_calculator_1 | 17:48:24 - Received 399-6529392b from dist_mean_calculator_1
dist_mean_calculator_1 | 17:48:24 - Bad packet: expected 397 or 398, got 399 from distance_calculator_1
```

> The logs have been edited for readability.

Here we can see that `distance_calculator_1` "sends" message `398` and `399`, not dying in between.
But `dist_mean_calculator_1` never receives `399`.

Thus, we determined that the packet was not being effectively sent.

### Solution

As it turns out pika, on which our middleware is based, does not ensure messages are sent by default.

Rather, it needs to be configured to do so, we have done so by:

- Turning delivery confirmations on using `channel.confirm_delivery`.
- Ensuring message delivery with the mandatory flag.

```python
class Rabbit(MessageQueue):
    def __init__(self, host: str):
        self._connection_params = pika.ConnectionParameters(host=host, heartbeat=0)
        self.connection = pika.BlockingConnection(self._connection_params)
        self._channel = self.connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        self._channel.confirm_delivery()
        ...

    def publish(self, event: str, message: bytes, confirm: bool = True):
        self.__declare_exchange(event, "fanout")
        self._channel.basic_publish(exchange=event, routing_key='', mandatory=confirm, body=message)
```
