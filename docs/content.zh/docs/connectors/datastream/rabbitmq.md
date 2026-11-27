---
title: RabbitMQ
weight: 7
type: docs
aliases:
  - /zh/dev/connectors/rabbitmq.html
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

# RabbitMQ 连接器

此版本支持使用 JDK 17 在 Flink 1.20.5 和 2.3.0 上运行新的 Java DataStream 作业。
使用 `org.apache.flink:flink-connector-rabbitmq`，并选择与 Flink 版本匹配的连接器版本：`3.1.0-1.20-SNAPSHOT` 或 `3.1.0-2.3-SNAPSHOT`。
`flink-sql-connector-rabbitmq` 包含 RabbitMQ Java 客户端，但没有 SQL 表工厂。

## RabbitMQ Source

通过 `RMQSource.addTo(...)` 添加 Source 并验证检查点配置：

- `AT_LEAST_ONCE`（推荐）：开启 Flink 检查点。检查点完成后才确认消息。允许并行消费和重新投递。
- `EXACTLY_ONCE`（可选）：开启精确一次检查点，将 Source 并行度设置为 1，并为每条已发布的消息设置唯一且非空的 correlation ID。此模式只抑制恢复检查点后 RabbitMQ 对已处理消息的重新投递，不保证去除生产者重试产生的重复消息，也不保证端到端精确一次投递。
- `AUTO_ACK`：RabbitMQ 在投递时自动确认消息，不提供故障恢复保证。

请在调用 `addTo(...)` 前配置检查点，之后不要禁用检查点或降低其一致性级别。
直接调用 `env.fromSource(...)` 不会执行连接器的检查点验证。
仅支持用相同 Flink 版本和 Source 并行度恢复此版本连接器写入的状态。
不支持旧版 Savepoint 或跨版本迁移。

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

### 安全处理重试

为每个逻辑事件分配稳定的事件 ID，并在重试时重用该 ID。
在幂等目标中，在同一事务内记录事件 ID 并执行相应的业务操作。
如果重复事件会更改 Flink 状态，请在有状态操作之前去重。

## RabbitMQ Sink

通过 `stream.sinkTo(...)` 使用 `RMQSink`：

```java
stream.sinkTo(new RMQSink<>(connectionConfig, "output", new SimpleStringSchema()));
```

Sink 不使用发布确认，也不会按检查点提交发布操作，因此不保证向 RabbitMQ 至少一次投递。

{{< top >}}
