## Error: Incorrect Results

### Summary

An error was detected where the system would output different results between runs.

We discovered packets were being marked as duplicates when they were not, and thus being discarded.

The source of the error was traced to a faulty logic when generating packet ids and detecting duplicate packets in scenarios where messages were being published ( sent to all following replicas )

### Analysis

#### Error Detection

Initially, the error was detected when comparing results between two runs.
One where processes were killed and one where they were not, which had 2 extra counts.
This made it seem like the problem stemmed from the fault tolerance mechanisms generating duplicate packets.

However, after comparing both results with the baseline, we realized that the problem was in the results without faults. Packets were being dropped rather than duplicated.

#### Error Analysis

Running the system again multiple times, we managed to reproduce the error.
In the system logs, we noticed the following:
```bash
tp2-trips_counter_1-1 | 21:29:46 - DUPL - (...) year_filter_0 - (...) - 2
tp2-trips_counter_1-1 | 21:29:46 - DUPL - (...) year_filter_1 - (...) - 2
tp2-trips_counter_0-1 | 21:29:46 - DUPL - (...) year_filter_0 - (...) - 2
tp2-trips_counter_0-1 | 21:29:46 - DUPL - (...) year_filter_1 - (...) - 2
```

Given that no processes where being killed, no duplicates where being generated.
Yet, the system was detecting duplicates and dropping packets.

We noted in the logs that the duplicates
- Were being 'generated' at the same time
- Had low packet ids (2)

These two observations led us to believe that the problem was not only in the duplicate detection mechanism, but also in the packet id generation mechanism for messages being published.

#### Code Analysis

##### Packet Id Generation

```python
# message_sender.py

def __get_next_seq_number(self, queue: str) -> int:
        queue_prefix = queue[:-2]
        if queue not in self._last_seq_number and queue_prefix in self._last_seq_number:
            self._last_seq_number[queue] = self._last_seq_number[queue_prefix]
        else:
            self._last_seq_number.setdefault(queue, 0)
        self._last_seq_number[queue] += 1

        if self._last_seq_number[queue] > MAX_SEQ_NUMBER:
            self._last_seq_number[queue] = 0
            self._times_maxed_seq += 1
            log_msg("Generated %d packets [%d]", MAX_SEQ_NUMBER, self._times_maxed_seq)

        return self._last_seq_number[queue]

def __get_next_publish_seq_number(self, queue: str) -> int:
    self._last_seq_number.setdefault(queue, 0)
    keys = list(self._last_seq_number.keys())
    keys = [key for key in keys if key.startswith(queue)]
    used_ids = [self._last_seq_number[key] for key in keys]

    possible_id = 0
    for i in range(MAX_SEQ_NUMBER):
        if possible_id not in used_ids:
            break
        possible_id += 1

    if possible_id == MAX_SEQ_NUMBER:
        raise Exception("No more publish ids available")

    for key in keys:
        self._last_seq_number[key] = possible_id

    return possible_id
```

- `__get_next_seq_number` is used for generating packet ids for messages being sent to a single replica, increasing the sequence number for each message sent.
- `__get_next_publish_seq_number` is used for generating packet ids for messages being sent to all replicas, searching for the first available id in all queues. Later on, all queues are updated to **use the same id**.


##### Duplicate Detection

```python
# last_received.py

def update(self, packet: GenericPacket) -> bool:
    sender_id = packet.sender_id

    current_id = packet.get_id()
    self._last_received.setdefault(sender_id, [None, None])
    last_chunk_id, last_eof_id = self._last_received[sender_id]

    if packet.is_eof():
        if current_id == last_eof_id:
            log_duplicate(
                f"Received duplicate EOF {sender_id}-{current_id}-{min_hash(packet.data)} - ignoring")
            return False
        self._last_received[sender_id][1] = current_id
    elif packet.is_chunk():
        if current_id == last_chunk_id:
            log_duplicate(
                f"Received duplicate chunk {sender_id}-{current_id}-{min_hash(packet.data)} - ignoring")
            return False
        self._last_received[sender_id][0] = current_id

    trace(
        f"Received {sender_id}-{current_id}-{min_hash(packet.data)}")

    return True
```

Here we see that the last packet sequence number is being updated for each sender, **keeping track of the last chunk and eof ids received independently**.

#### Error

The error stems from
- Generating a common id when publishing messages
- Keeping track of the last chunk and eof ids independently when detecting duplicates

For example, take the following scenario:
- Node a_0 has _last_seq_number: `{ b_0: 0, b_1: 1 }`
- Node a_0 sends a message to b_1
  - Updating _last_seq_number to `{ b_0: 0, b_1: 2 }`
  - Node b_1 receives the message and updates _last_received to `{ a_0: [2, None] }`
- Node a_0 has to **publish** an eof:
  - It determines the possible id as `1`
  - Updates _last_seq_number to `{ b_0: 1, b_1: 1 }`
  -  Node b_1 receives the message and updates _last_received to `{ a_0: [2, 1] }`
- Node a_0 sends a message to b_1
  - Updating _last_seq_number to `{ b_0: 0, b_1: 2 }`
  - Node b_1 receives the message and **detects it as duplicate**,
    because the last chunk id is `2` and the current message id is `2`

### Solution

The proposed solution is to refactor the code that generates packet ids for messages being published to all replicas.

Rather than checking and updating the numbers generated for 'direct' messages to avoid overlapping, we can generate negative ids for published messages, thus:
- Avoiding id overlapping between published and direct messages
- Avoiding errors from repeated eof/chunk ids

This ensures that each message has a unique id, even when sent to multiple replicas.

```python
# message_sender.py

def __get_next_publish_seq_number(self, queue: str) -> int:
    self._last_seq_number.setdefault(queue, -1)
    self._last_seq_number[queue] -= 1

    if self._last_seq_number[queue] < -MAX_SEQ_NUMBER:
        self._last_seq_number[queue] = -1
        self._times_maxed_seq += 1
        log_msg("Generated %d packets [%d]", MAX_SEQ_NUMBER, self._times_maxed_seq)

    return self._last_seq_number[queue]
```
> Implementation of the proposed solution

### Lessons Learned

We've concluded that this oversight was caused due to the lack of more effective testing.

Further feature development was prioritized over testing tools and mechanisms.

The existence of automated and smaller scale tests would have surfaced errors like this one, and would have allowed us to fix them earlier on.