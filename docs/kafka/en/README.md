# Understanding Kafka from First Principles

[English](README.md) | [繁體中文](../zh-TW/README.md)

## From “Why Do We Need It?” to Understanding Your First Kafka Issue

**Version B | Short Main Path v1.1 | Prototype for Review**

- **Status:** Prototype for Review
- **Technical baseline:** Apache Kafka 4.3.1
- **Language:** English
- **Author:** Yen-Hua Chen
- **Original source:** [https://github.com/hikaru-212/open-source-from-first-principles](https://github.com/hikaru-212/open-source-from-first-principles)
- **License:** CC BY 4.0

> Version scope: This document uses Apache Kafka 4.3.1 as its technical baseline. Current Kafka servers use KRaft; ZooKeeper belongs only to historical and migration topics and is not included in the beginner main path.

### Document Positioning and Version Scope

This document is a teaching prototype based technically on Apache Kafka 4.3.1. Its goal is to establish a core systems mental model of Kafka and provide the shortest introductory path from understanding mechanisms to reading issues, tests, and source code.

To control the cognitive load for beginners, this document deliberately omits some protocol details, historical architectures, and advanced mechanisms. Its simplifications should not be treated as the complete Kafka specification. If this document differs from the official documentation, Javadoc, or source code for the corresponding Apache Kafka version, the official material takes precedence.

Deep Dives and advanced implementation details are reserved for later expanded versions.

---

# Chapter 1 — Why Kafka?

## What Happens If the Producer and Consumer Must Be Alive at the Same Time?

Suppose an order service must notify three systems after a payment is completed:

```
Order service
├─→ Inventory system
├─→ Notification system
└─→ Analytics system
```

The most intuitive approach is for the order service to call all three APIs synchronously.

When traffic is low and every service is healthy, this design is simple. But several physical problems soon appear:

- If the notification system is down, should the order fail too?
- If the analytics system is ten times slower, must the order request wait with it?
- If a fraud-detection service is added six months later, how does it obtain past orders?
- If the analytics logic is wrong and yesterday’s data must be recomputed, do the original events still exist?

Adding timeouts and retries can ease some symptoms, but it does not change the fundamental coupling: in this synchronous, directly connected design, the Producer’s success path is still tied to each Consumer’s current availability and speed.

What we actually need is an intermediary that can retain events:

```
Order service
    ↓
Event-retaining intermediary
    ├─→ Inventory system
    ├─→ Notification system
    └─→ Analytics system
```

It must preserve at least three conditions:

1. After an event is written, it does not disappear because one Consumer is temporarily offline.
2. Each Consumer can advance at its own pace.
3. Older data can still be read again during its retention period.

Only now do we need names: a program that writes data is called a **producer**, a program that reads data is called a **consumer**, and Kafka is the system in the middle that stores and provides the data.

As a beginner, you can think of Kafka as:

> A set of distributed, durably retained logs.

But this is only a starting point. Kafka is not one infinitely large log, and it does not retain all data forever. It stores multiple logs that can be partitioned and replicated and that are bounded by retention rules.

Kafka can prove that a record has crossed a particular write and replication boundary, and it can let a Consumer retrieve that record again during the retention period. Kafka cannot thereby prove that inventory was deducted, a notification was delivered, or the event contents are necessarily correct in business terms.

## Checkpoint

If the analytics system is offline for two hours but the order service must continue working, what state must the system retain so that the analytics system can resume from its own progress after recovery, without asking the order service to resend all data?

## Deep Dive (Optional)

- Retention policies and storage capacity
- The respective problems solved by Kafka Connect and Kafka Streams

---

# Chapter 2 — What Must Be Ordered Together?

## Which Events Actually Need the Same Order?

Suppose we want every event to have a clear order. The most intuitive design is to send all of them into one lane:

```
Producer A ─┐
Producer B ─┼─→ One global order ─→ Consumer
Producer C ─┘
```

This does produce a total order, but the cost is equally direct: every Producer must pass through the same ordering point. Work that was originally unrelated is also forced to wait in the same queue.

Orders in Taipei and Kaohsiung might be completely independent, yet they still have to compete for the same lane. As traffic grows, this shared ordering point becomes a coordination cost, a serialization bottleneck, and a concentrated point of failure at the same time.

So the question we should really ask is not:

> How can every event have the same order?

It is:

> Which events actually have to be ordered together?

For example, the states of one order may have to remain in this order:

```
created → paid → shipped
```

But order A and order B do not necessarily need to be ordered relative to each other.

Kafka’s design choice is to divide a topic into multiple **partitions**. Each partition is an independent ordering domain:

```
partition 0: A-created → A-paid → A-shipped
partition 1: B-created → B-paid → B-cancelled
```

Kafka guarantees log order within a partition; Kafka provides no total order across different partitions. Comparing offset 100 in `partition 0` with offset 80 in `partition 1` does not tell us which happened first globally.

Only then do we need a **key**. If events for the same order must remain in the same ordering domain, `order_id` may be a reasonable key. The Producer’s partition-selection rules use the key to decide which partition receives the data.

But “the same key always goes to the same partition” requires preconditions:

- another partition is not specified explicitly;
- a different custom partitioner is not used;
- the key is not configured to be ignored;
- the serializer and partition count remain compatible.

In particular, after the partition count increases, the mapping for future records with the same key may change. Expanding the number of partitions is not only a performance operation; it can also affect an ordering domain on which the application relied.

From the perspective of ordering, the most important value of a Partition to understand first is not merely that it divides data. It lets different ordering domains advance independently, so only data that truly needs a shared order bears the ordering cost together.

## Checkpoint

In a package-tracking system, each package has “received,” “transferred,” “out for delivery,” and “delivered” events. What would you choose as the key? What kind of order would that choice guarantee, and what global order would it deliberately give up?

## Deep Dive (Optional)

- Hot partitions and data skew
- Custom partitioners
- Key migration when expanding partitions

---

# Chapter 3 — Key, Partition, Offset, Consumer Group

## Which Lane Holds the Data? How Far Have We Read? Who Is Responsible Now?

Do not start by memorizing four terms. Start by answering three physical questions.

### Question 1: Which Lane Does This Record Belong To?

When a Producer sends a record, partition-selection rules choose a partition based on the key, an explicitly specified partition, or other configuration.

A Key is not itself an order. It is an input for selecting an ordering domain; the partition log is what actually provides the order.

### Question 2: How Far Along This Lane Are We?

Within each partition, Kafka assigns an **offset** to each record:

```
partition 0: ... 41, 42, 43
partition 1: ...  8,  9, 10
```

An Offset is a partition-local logical position, not global time or a sequence number shared across the cluster. It represents the record’s position in this log; it does not represent when a business operation completed.

A Consumer also has two forms of progress that are easy to confuse:

- **consumer position**: the position this Consumer is preparing to read or deliver next;
- **committed offset**: the recovery checkpoint stored by the Consumer group, usually indicating where the next read should begin after a restart.

After `poll` obtains data, the consumer position can advance even though the program may not have finished a database write or HTTP call. Position is therefore not proof of completion.

A Committed offset is not proof of completion either. It only represents the application telling Kafka: “When recovering in the future, start from here.” Kafka does not independently verify whether the earlier external work actually completed.

### Question 3: Who Is Responsible for This Lane Now?

Multiple Consumers using the same `group.id` form an ordinary **consumer group**. Kafka assigns partitions to group members. Under the ordinary consumer group assignment semantics discussed in this document, at a given time one partition is assigned to one active Consumer in that group. Kafka Share Groups are a different consumption model and are outside this document’s main path.

Therefore:

- Fewer Consumers than partitions: one Consumer may be responsible for multiple lanes.
- More Consumers than partitions: some Consumers have no partition to process.
- Different consumer groups: each can read the same topic in full and maintain its own progress.

Kafka coordinates partition ownership and recovery position. That does not give it control over an external database, HTTP API, or background thread.

### See It in Failure | Ownership Handoff Does Not Mean Old Work Stops

When ownership transfers within a Consumer group, begin with this timeline:

```text
Consumer A owns partition P
        ↓
A polls a record and starts external work
        ↓
A stops heartbeating / loses ownership
        ↓
Kafka assigns P to Consumer B
        ↓
B recovers from the committed offset
        ↓
Old external work started by A may still complete
```

Kafka can cause the old Consumer to lose group ownership of the partition, and it can reject some stale group operations. But it cannot automatically cancel an HTTP request, SQL transaction, or background thread that A has already started. In other words, **ownership transfer does not mean old work disappeared**. This is why recovery cannot look only at “who owns the partition now”; it must also ask which effects the previous owner had already sent.

### See It in Code | Put the Progress Boundary Back into Program Execution

The following is not a complete Java example. It is deliberately simplified pseudocode:

```python
records = consumer.poll(...)
for record in records:
    handle(record)        # external effect may already succeed

# ───── failure window ─────
# crash here: effect may exist, but committed offset has not advanced

consumer.commitSync()
```

The important thing is not the API syntax but the two points in time. After `poll()`, the Consumer’s local position may already have advanced. Before `commitSync()` completes, however, the committed offset stored by the Consumer group may still be at its old position.

Therefore, if `handle(record)` has completed an external database or HTTP operation but the program crashes before the commit, a new Consumer may still retrieve the same record from the old checkpoint. This is why “read,” “processed,” and “Kafka has stored the recovery progress” cannot be collapsed into one state.

## Checkpoint

Here, the committed offset represents “the next position to read” when the Consumer group recovers. A Consumer has obtained offsets 80–89 but has committed only offset 85, then crashes. Where will the Consumer that takes over begin? Which records may appear again? Can Kafka know which external work has actually completed?

## Deep Dive (Optional)

- Classic and new consumer rebalance protocols
- Rebalance protocol internals and static membership
- High watermark and last stable offset

---

# Chapter 4 — What Happens When Machines Fail?

## After a Timeout, What Do We Actually Know?

One of the most dangerous statements in distributed systems is:

> I received an error, so the operation definitely did not succeed.

Consider this timeline:

```
Producer sends a record
        ↓
Broker leader writes it and completes the required replication
        ↓
Broker returns an acknowledgement
        ↓
The response is lost in the network
        ↓
Producer times out
```

The Producer sees a timeout, but the record may already exist and may already be visible to a Consumer.

So the correct answer to “Did the write fail?” is:

> From the timeout alone, we do not know.

The Producer’s `acks` setting determines which boundary a successful response has crossed:

- `acks=1`: the leader can respond after writing to its own log. If it fails before replication, an acknowledged record may still be lost.
- `acks=all`: the leader waits for all replicas currently in the ISR to acknowledge. If the current ISR size is below `min.insync.replicas`, the write fails. This is the strongest acknowledgement setting Kafka can provide, but it still does not mean that all assigned replicas, a disk fsync, or downstream business processing has completed.

When the outcome is uncertain, the Producer may retry. The question is: did the first attempt actually succeed? If it did, will sending again create a duplicate?

Kafka’s **idempotent producer** uses producer identity and partition sequence state to recognize retries of the same protocol batch and prevent it from being appended twice. This is important but bounded protection:

- It protects qualifying Kafka producer retries.
- It does not recognize whether a separate `send` later created by the application represents the same business operation.
- It does not deduplicate external APIs or databases.

The Consumer side has the same kind of uncertainty:

```
Consumer reads a record
        ↓
External effect succeeds
        ↓
Consumer crashes
        ↓
Offset has not been committed
        ↓
Another Consumer reads the record again
```

Kafka knows that the committed offset has not advanced, so it will deliver the record again. It does not know whether the earlier email, charge, or database update completed. Conversely, if the offset is committed before the external effect and a crash occurs between them, that effect may be skipped forever.

Failure is not one Boolean value. We must ask separately: which actor completed which step? Which state was stored? Who can see it? Who saw only a timeout? Which outcomes remain uncertain?

### See It in Config | First Recognize the Three Names You Will Encounter Most Often

You do not need to learn tuning first in this chapter. But when you later see Producer configuration, you should at least be able to map these names back to their questions:

- `acks`: which broker-side acknowledgement/replication boundary a successful response must cross.
- `enable.idempotence`: lets the Producer use identity and sequence state to suppress duplicate appends from qualifying protocol retries; it is not business-level idempotency.
- `retries`: whether the Client may resend a request after a retriable error. It answers “will it try again,” not “is this retry safe in business terms.”

These three settings are not three independent magic switches. Before understanding them, return to the failure timeline: how far might the previous attempt have progressed? What evidence remains now?

## Checkpoint

When a Producer times out, identify separately what it knows and does not know. Why is “directly creating another new send” not necessarily equivalent to a protocol retry that Kafka can safely deduplicate?

## Deep Dive (Optional)

- ISR and Eligible Leader Replicas
- Producer sequence, epoch, and fencing
- `min.insync.replicas`

---

# Chapter 5 — Three Dangerous Kafka Misunderstandings

## Misunderstanding 1: Partition Order Is Global Order

Kafka’s ordering exists within a partition, not across every partition in a topic.

If order A is in partition 0 and order B is in partition 1, their offsets cannot be compared across partitions. Even if both records carry timestamps, those timestamps may be affected by clocks, network delays, and Producer behavior; they do not automatically become a Kafka guarantee of global order.

A safe statement is:

> Kafka can establish log order for records placed in the same partition.

It cannot be expanded into:

> Kafka knows the true order in which every event in the entire system occurred.

## Misunderstanding 2: Kafka Success Is Business Completion

Every form of success returned by Kafka proves only one bounded fact.

| Technical evidence | What it can prove | What it cannot prove |
| --- | --- | --- |
| Producer acknowledgement | The record crossed the configured broker write/replication condition | A Consumer read it; the order completed |
| Committed offset | The Consumer group stored a new recovery position | An HTTP, database, or other effect completed |
| Consumer received a record | The record was delivered to this processing attempt | Processing necessarily succeeded; the record will never be redelivered |

When you see technical success, first ask: “Evidence from which actor, responsible for which segment of state?” Do not translate it directly into business success.

## Misunderstanding 3: Idempotence, Transactions, and Replay Mean Exactly-Once-Everything

All three mechanisms are valuable, but their boundaries differ:

- Producer idempotence prevents particular Kafka retries from creating duplicate appends.
- Kafka transactions can coordinate Kafka records and Kafka consumer offsets together.
- `replay` lets a Consumer read data again while it is still retained.

None of them means “an arbitrary business action happens exactly once.”

A Kafka transaction does not automatically include an ordinary HTTP API or external database in the same atomic boundary. Even when Kafka’s internal records and offsets commit together, an external effect may still need a separate design.

Replay is not a time machine either. Retention can delete old data; compaction can remove older values for the same key; necessary information such as tombstones and schemas may also no longer exist. Whether state can be rebuilt depends on whether the required evidence is still retained and can be interpreted.

## Checkpoint

Someone says, “We use a Kafka transaction, so the database charge, Kafka output record, and consumer offset will definitely be exactly once.” What atomicity assumption hidden in this statement has not yet been established?

## Deep Dive (Optional)

- Kafka transaction coordinator, transaction markers, and LSO
- Transactional consume-transform-produce
- Outbox/inbox patterns

---

# Chapter 6 — From Kafka User to Kafka Contributor

## From One Guarantee to Source, Test, and Issue

Becoming Contribution Ready does not mean reading the entire Kafka repository first.

Kafka spans clients, brokers, storage, replication, group coordination, transactions, and the KRaft controller. If you begin by following the complete broker request path downward, it is easy to see many classes without knowing which behavior represents the contract you are trying to verify.

A more effective entry point is one bounded guarantee:

```
Public contract
    ↓
Configuration and constraints
    ↓
Focused test
    ↓
Internal state machine
    ↓
Issue or minimal fix
```

Your first entry into the source does not need to begin with the most complex state machine. Start with a lighter path:

### Beginner Path | Start with a Client Internal That Maps Clearly to Public Behavior

```
KafkaProducer Javadoc
        ↓
ProducerConfig
        ↓
KafkaProducerTest / focused producer test
        ↓
RecordAccumulator
```

This path first lets you observe how the Producer connects public configuration to the internal behavior of record batching. The goal is not to understand every detail of `RecordAccumulator` at once. It is to practice mapping a public contract to a focused test and then finding the implementation responsible for that state.

### Advanced Path | When You Are Actually Studying Retry, Idempotence, or Transactions

```
KafkaProducer Javadoc
        ↓
ProducerConfig
        ↓
TransactionManagerTest
        ↓
TransactionManager
```

`TransactionManager` involves producer identity, sequence, epoch, fencing, and transaction state. It is valuable, but it does not need to be the only entry point for someone reading Kafka source for the first time.

Javadoc tells you the public promises. Configuration explains which behaviors depend on settings. A focused test turns a promise into an executable case. Only then do you read the internal state machine that implements it.

When facing a suspected bug, ask in order:

1. What does the public documentation actually promise?
2. Which physical actors are involved in this behavior?
3. Which state transition is expected?
4. At which step can a Failure interrupt it?
5. What persisted evidence remains after the interruption?
6. Which focused test most closely matches this contract?
7. Only then enter the implementation to find the cause or modification point.

This process avoids two common mistakes: seeing an exception and immediately guessing the root cause; or modifying suspicious code without first confirming that it violates a public guarantee.

Kafka currently tracks issues in **JIRA** and uses **GitHub** for code and Pull Request review. Changes that affect public APIs, protocols, configuration, or major behavior usually require **KIP** discussion and voting. A small bug fix does not automatically require a KIP merely because it changes Kafka.

A first contribution can be only:

- a reproducible failure timeline;
- a focused test that exposes a difference in the contract;
- an investigation that clearly separates “observation” from “suspected cause”;
- a very small, well-supported fix.

Do not assume an issue remains suitable merely because JIRA gives it a `newbie` label. Also confirm that the issue is still valid, no PR already exists, it can be reproduced on the current version, and its modification scope is genuinely limited.

> Contribution Ready does not mean knowing all of Kafka.
> It means being able to trace one bounded behavior from guarantee to evidence to implementation.

## Checkpoint

If an issue claims that “a Producer timeout causes a duplicate,” what evidence would you establish before opening the implementation? At minimum, which public promises, relevant settings, failure timeline, and focused test should you confirm?

## Deep Dive (Optional)

- Kafka protocol message schemas
- Source topology of the Broker, storage, and KRaft controller
- Integration tests and system tests

---

# Technical Basis and Further Reading

## Chapters 1–2: Motivation, Partitions, and Ordering

- [Apache Kafka 4.3 Introduction](https://kafka.apache.org/43/getting-started/introduction/)
- [Apache Kafka 4.3 Design](https://kafka.apache.org/43/design/design/)
- [Producer Configuration](https://kafka.apache.org/43/generated/producer_config.html)
- [Basic Kafka Operations](https://kafka.apache.org/43/operations/basic-kafka-operations/)

## Chapter 3: Offsets, Consumer Progress, and Group Ownership

- [KafkaConsumer Javadoc](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
- [Consumer Configuration](https://kafka.apache.org/43/generated/consumer_config.html)
- [Consumer Rebalance Protocol](https://kafka.apache.org/43/operations/consumer-rebalance-protocol/)
- [KIP-848: The Next Generation of the Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)

## Chapters 4–5: Acknowledgement, Failure, Transactions, and Replay

- [KafkaProducer Javadoc](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
- [Apache Kafka Design: Replication and Transactions](https://kafka.apache.org/43/design/design/)
- [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
- [Topic Retention and Compaction Configuration](https://kafka.apache.org/43/configuration/topic-configs/)

## Chapter 6: Source and Contribution Workflow

- [Apache Kafka Developer Guide](https://kafka.apache.org/community/developer/)
- [Contributing Code Changes](https://cwiki.apache.org/confluence/display/KAFKA/Contributing+Code+Changes)
- [Kafka Improvement Proposals](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals)
- [KafkaProducer source area — Kafka 4.3.1](https://github.com/apache/kafka/tree/4.3.1/clients/src/main/java/org/apache/kafka/clients/producer)
- [KafkaProducerTest — Kafka 4.3.1](https://github.com/apache/kafka/blob/4.3.1/clients/src/test/java/org/apache/kafka/clients/producer/KafkaProducerTest.java)
- [RecordAccumulator — Kafka 4.3.1](https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java)
- [TransactionManagerTest — Kafka 4.3.1](https://github.com/apache/kafka/blob/4.3.1/clients/src/test/java/org/apache/kafka/clients/producer/internals/TransactionManagerTest.java)
- [TransactionManager — Kafka 4.3.1](https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java)

---

## Methodology Provenance

The first-principles teaching approach used in this guide grew out of the engineering reasoning documented by **Yen-Hua Chen** in **Streaming System + Compass**, including ADRs, postmortems, reasoning notes, and system-design analysis.

The Kafka track does not directly apply Compass architecture to Kafka. The reasoning patterns used here were independently checked against Apache Kafka documentation, Javadocs, KIPs, source code, and tests; analogies were rejected when the underlying physical mechanisms did not actually match.

This provenance describes the origin of the teaching methodology and framing. It does not imply that Kafka's architecture or design originated from Compass.

**Streaming System + Compass** — Yen-Hua Chen\
Source: [https://github.com/hikaru-212/streaming-system-compass](https://github.com/hikaru-212/streaming-system-compass)\
Documentation license: CC BY 4.0

## Author’s Short Note

- The main path deliberately omits: KRaft internals, ISR/ELR details, the complete transaction protocol, the LSO algorithm, Share Consumer, Kafka Streams/Connect, security, schema evolution, and administrative operations.
- This version adds a small number of executable bridges: pseudocode, key configuration names, and two source-reading paths connect abstract concepts back to real engineering surfaces without expanding into an API tutorial.
- v1.1 further visualizes two failure boundaries: the crash window from `handle → commit`, and the handoff window in which old external work may still complete after consumer ownership transfers.
- An expanded version could add: hands-on failure labs, deeper rebalance analysis, outbox/inbox patterns for external databases, compaction rebuild conditions, more source/test reading paths, and newcomer issues verified in practice.
