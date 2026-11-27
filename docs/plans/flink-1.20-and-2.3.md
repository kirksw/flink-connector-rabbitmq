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

# RabbitMQ connector for Flink 1.20 and 2.3

## Scope and evidence

Support new Java DataStream jobs on Flink 1.20.5 and 2.3.0, with one codebase and one tested artifact per Flink line.
Preserve the documented source guarantees and sink behavior; do not require legacy constructors, restoration from legacy savepoints, or state portability between Flink majors.
Recovery of a job started with the new connector on the same target is required.
SQL table discovery, PyFlink wrappers, a RabbitMQ client upgrade, and stronger sink delivery guarantees are separate work.

- [Apache downloads](https://flink.apache.org/downloads/) and [Maven Central](https://repo.maven.apache.org/maven2/org/apache/flink/flink-core/maven-metadata.xml) identify the target releases; Apache lists RabbitMQ connector 3.0.1 for Flink 1.16.x and 1.17.x only.
- At the start of the port, `pom.xml` defaulted to 1.16.0 and `.github/workflows/push_pr.yml` targeted 1.18 and 1.19 snapshots.
- The [Flink 2.0 release notes](https://nightlies.apache.org/flink/flink-docs-release-2.0/release-notes/flink-2.0/) retire the legacy APIs used by `RMQSource` and `RMQSink`.
- Sink V2's required writer factory differs in [1.20.5](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-core/src/main/java/org/apache/flink/api/connector/sink2/Sink.java#L68) and [2.3.0](https://github.com/apache/flink/blob/c0f8d1a1e09f209885a88f9c19ceb9d9e9870283/flink-core/src/main/java/org/apache/flink/api/connector/sink2/Sink.java#L47).
- [Upstream PR #1](https://github.com/apache/flink-connector-rabbitmq/pull/1) explored the modern source but was not merged.

## Implementation order and acceptance gates

1. **Build the local test environment first.** Add `flake.nix`, generate and commit `flake.lock`, and expose JDK 17, Maven, RabbitMQ with compatible Erlang, and both exact Flink distributions (fixed hashes).
   First target `aarch64-darwin` for local development and `x86_64-linux` for CI; verify package availability and launch behavior on each before declaring support.
   Provide `nix run .#e2e -- --flink 1.20.5`, `nix run .#e2e -- --flink 2.3.0`, and `nix run .#e2e` for the full matrix; expose the same tools via `nix develop` for debugging.
   The first acceptance gate is a submitted control job that finishes on each target and an independent AMQP publish/consume check, without a system JDK, Maven, Docker daemon, or existing broker.
   Document first-run network requirements for Nix inputs, distributions, and Maven dependencies; do not promise an offline build from an empty cache.
2. **Prove the artifact boundary.** Build separate version-qualified Maven artifacts from shared sources; use target-specific source sets or modules only where the public APIs differ.
   Compile a small external job against each artifact, with Flink dependencies provided by the cluster, and submit it to the matching standalone distribution.
   Keep `flink-sql-connector-rabbitmq` as a shaded distribution rather than claiming SQL table support; separately smoke-test its relocated AMQP client without putting both connector jars on one classpath.
3. **Port the source to FLIP-27.** Use `env.fromSource(...)`, an unbounded source, a split enumerator, a nonblocking reader, and versioned split and enumerator serializers.
   Represent parallel consumers of one queue as logical assignments, not disjoint queue offsets; assign one consumer in the exactly-once case.
   Bridge asynchronous deliveries to the reader without blocking `pollNext`; preserve prefetch, custom deserialization and cancellation.
   Verify how each target exposes checkpoint configuration; if it cannot reliably be inferred through public APIs, expose an explicit acknowledgement mode and reject unsafe combinations rather than silently auto-acknowledging.
   Snapshot stable deduplication IDs, never session delivery tags; associate live tags with their channel and checkpoint, and acknowledge only after checkpoint completion.
   Handle skipped or aborted checkpoints and redeliveries; do not discard deduplication evidence while the broker can still redeliver the corresponding message.
4. **Port the sink to Sink V2.** Use `sinkTo(...)` and share the writer's serialization, queue setup, routing, properties, returns and failure policy between targets.
   Isolate writer initialization differences in the two adapters and test resource cleanup on shutdown and failure.
   Publishing is not a checkpoint transaction; neither source exactly-once processing nor a successful round trip implies end-to-end exactly-once delivery to RabbitMQ.
5. **Gate release on the local E2E suite below.** Run both target commands in CI using the same flake and runner developers use locally.
   Keep unit tests for quick feedback; update `.github/workflows/push_pr.yml`, `.github/workflows/weekly.yml`, version metadata, and the Java examples once both packaged artifacts pass.
   Document supported Flink/JDK combinations, delivery modes and coordinates, and explicitly exclude legacy savepoint migration.

## Local end-to-end suite

Build a small external job and independent AMQP test driver under `e2e/`.
The legacy `RMQSourceITCase` used a MiniCluster and Testcontainers; the new suite verifies that the packaged artifact loads in the standalone Flink distribution.
Run the networked suite as a flake app outside the Nix build sandbox; reserve `nix flake check` for sandbox-compatible packaging and static checks.

For each run, build the target artifacts, start a private native RabbitMQ node and standalone Flink JobManager and TaskManager on loopback, then submit the job through the Flink CLI or REST API.
Use writable per-run configs, broker data, Erlang cookie, checkpoint directory, logs and ports, not the Nix store or global service state.
Wait for broker, task slots and job readiness with bounded deadlines; clean up only this run's processes on success, failure or interruption.
Return a nonzero exit on assertion failure or timeout and print artifact versions, job ID, checkpoint IDs, broker observations, and the retained log directory.

| Scenario | What must be observable on each target |
| --- | --- |
| Packaged round trip | Independently publish numbered inputs; the deployed job reads and routes them; an independent AMQP consumer verifies payloads, routing and properties on the output queue. |
| Exactly-once source recovery | With one reader, checkpoints, and unique correlation IDs, pause the reader at a deterministic test hook after checkpoint completion but before the broker acknowledgement commits. Kill the TaskManager, restart it against the same broker, prove redelivery occurred, and assert one logical result per ID in a checkpoint-consistent test result, not in the ordinary RabbitMQ sink. |
| At-least-once recovery | Run without correlation IDs and separately with parallel consumers. Force redelivery, then verify that all input IDs appear; duplicates are permitted. |
| Checkpoint edge cases | Abort a checkpoint with pending deliveries and prove they are not acknowledged early. Complete a later checkpoint and verify pending acknowledgements clear. Reconnect and verify no old-channel tags are acknowledged on the new channel. |
| Non-checkpointed and failure modes | Verify auto-ack behavior without claiming recovery guarantees. Exercise missing required correlation IDs, unroutable mandatory publishes with a return listener, and the configured sink failure policy. |
| Lifecycle and repeatability | Stop and restart a new-connector job from its own supported state on the same target. Run the suite twice without manual cleanup, and check that failed runs release ports and leave useful logs. |

Use checkpoint IDs and explicit signals to coordinate failure injection, not sleep durations.
For the exactly-once oracle, use a test-only checkpointed counting operator and a final committed result that exposes counts greater than one; a set of IDs or an idempotent sink would hide duplicates.
If either target fails the recovery gate, do not publish an exactly-once claim for it.
