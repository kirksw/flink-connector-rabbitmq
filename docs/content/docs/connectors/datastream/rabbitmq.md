---
title: RabbitMQ
weight: 7
type: docs
aliases:
  - /dev/connectors/rabbitmq.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# RabbitMQ Connector

## License of the RabbitMQ Connector

Flink's RabbitMQ connector defines a Maven dependency on the
"RabbitMQ AMQP Java Client", is triple-licensed under the Mozilla Public License 1.1 ("MPL"), the GNU General Public License version 2 ("GPL") and the Apache License version 2 ("ASL").

Flink itself neither reuses source code from the "RabbitMQ AMQP Java Client"
nor packages binaries from the "RabbitMQ AMQP Java Client".

Users that create and publish derivative work based on Flink's
RabbitMQ connector (thereby re-distributing the "RabbitMQ AMQP Java Client")
must be aware that this may be subject to conditions declared in the Mozilla Public License 1.1 ("MPL"), the GNU General Public License version 2 ("GPL") and the Apache License version 2 ("ASL").

## Maven Dependency

For new Java DataStream jobs, use `org.apache.flink:flink-connector-rabbitmq` with the version that matches your cluster:

| Flink | Connector version | Java |
| --- | --- | --- |
| 1.20.5 | `3.1.0-1.20-SNAPSHOT` | 17 |
| 2.3.0 | `3.1.0-2.3-SNAPSHOT` | 17 |

These are development artifacts that you can build with `mvn clean install` or `mvn -Pflink-2.3 clean install`, respectively.
The `flink-sql-connector-rabbitmq` JAR bundles and relocates the RabbitMQ Java client but does not implement SQL table discovery.
Do not load the regular and shaded connector JARs together.

## RabbitMQ Source

Use `RMQSource.addTo(...)` to add the source and validate checkpoint settings.
A RabbitMQ queue distributes messages across competing consumers, not independent partitions with replayable offsets.
The source provides three explicit acknowledgement modes:

| Mode | Requirements and guarantees |
| --- | --- |
| `AT_LEAST_ONCE` (recommended) | Enable Flink checkpointing. The source acknowledges deliveries after a checkpoint completes. RabbitMQ can redeliver after a failure. Correlation IDs are not required, and parallel readers are supported. |
| `EXACTLY_ONCE` (opt-in) | Enable Flink exactly-once checkpoints, use one source reader, and set a unique, nonempty correlation ID on each published message. This mode suppresses broker redelivery of records already covered by the restored checkpoint. It does not generally deduplicate producer retries or guarantee exactly-once delivery to a destination. |
| `AUTO_ACK` | Checkpointing is not required. The broker can remove a delivery before the job has processed it; there is no recovery guarantee. |

Configure checkpointing before calling `addTo(...)`, and do not disable or weaken it afterwards.
If you call Flink's `env.fromSource(...)` directly, it does not perform the connector's checkpoint validation; a checkpointed mode will not acknowledge deliveries without completed checkpoints.
The source reconnects by failing the Flink task, so configure a restart strategy for recovery.
Producer correlation IDs are application-level identifiers; RabbitMQ delivery tags are scoped to one channel and are not checkpointed.
On restore, an ID for an acknowledgement that may already have succeeded is kept until its delivery can be resolved; RabbitMQ has no way to query the original tag from the new channel.
Restore only state created by this connector on the same supported Flink version and with the same source parallelism.
Legacy savepoints and cross-version state migration are not supported.

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(1000);

final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
        .setUri("amqp://guest:guest@localhost:5672/")
        .setPrefetchCount(1000)
        .build();

final DataStream<String> stream = new RMQSource<>(
        connectionConfig, "input", AcknowledgementMode.AT_LEAST_ONCE, new SimpleStringSchema())
        .addTo(env, WatermarkStrategy.noWatermarks(), "RabbitMQ input");
```

### Making Retries Safe

Give each logical event a stable event ID that producers reuse on retry.
At an idempotent destination, record the event ID and apply its business effect in the same transaction, such as inserting the ID under a unique constraint before updating a balance.
If duplicate events would change Flink state, deduplicate before the stateful operation; an idempotent sink cannot correct an inflated count.
The RabbitMQ sink in this connector is not an idempotent destination and does not guarantee at-least-once delivery to RabbitMQ.

### Consumer Prefetch

The source configures `basicQos` per consumer; the default is 1,000 if prefetch is unset.
Set `RMQConnectionConfig.Builder#setPrefetchCount` to adjust how many deliveries the broker can keep outstanding on each manually acknowledged consumer.
Prefetch does not limit automatically acknowledged deliveries.
For custom queue declarations or bindings, override `RMQSource#setupQueue(Channel, String)`.

## RabbitMQ Sink

Use `RMQSink` with `stream.sinkTo(...)`:

```java
stream.sinkTo(new RMQSink<>(connectionConfig, "output", new SimpleStringSchema()));
```

Use `RMQSinkPublishOptions` to select the exchange, routing key and message properties.
Mandatory publishes require a `SerializableReturnListener` to receive unroutable messages.
Publish errors fail the writer by default; `setLogFailuresOnly(true)` logs and ignores publish I/O failures.
The sink does not use publisher confirms or checkpoint-coordinated commits, and does not provide end-to-end exactly-once delivery to RabbitMQ.

{{< top >}}
