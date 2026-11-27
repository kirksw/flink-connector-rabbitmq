# Apache Flink RabbitMQ Connector

This repository contains the official Apache Flink RabbitMQ connector.

## Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Building the Apache Flink RabbitMQ Connector from Source

The connector targets new Java DataStream jobs on Flink 1.20.5 and 2.3.0 with JDK 17.
Select the artifact whose version suffix matches the Flink line:

| Flink | Connector version | Build profile |
| --- | --- | --- |
| 1.20.5 | `3.1.0-1.20-SNAPSHOT` | Default |
| 2.3.0 | `3.1.0-2.3-SNAPSHOT` | `-Pflink-2.3` |

The regular artifact is `org.apache.flink:flink-connector-rabbitmq`.
The `flink-sql-connector-rabbitmq` artifact bundles the RabbitMQ client but does not provide a SQL table factory.

Use JDK 17 and Maven, or enter the pinned environment with `nix develop`.

```sh
mvn clean install
mvn -Pflink-2.3 clean install
```

For reliable consumption, use the checkpointed `AT_LEAST_ONCE` source and make destination writes idempotent.
When using `RMQSource.addTo(...)`, enable checkpointing before adding the source.
The opt-in `EXACTLY_ONCE` mode deduplicates broker redelivery on checkpoint recovery; it does not deduplicate arbitrary producer retries or make the RabbitMQ sink exactly-once.
See `docs/content/docs/connectors/datastream/rabbitmq.md` for setup and retry guidance.

## Local End-to-End Tests

With Nix flakes enabled, run from the repository root:

```sh
nix run .#e2e                       # both Flink versions
nix run .#e2e -- --flink 1.20.5    # one version
nix run .#e2e -- --flink 2.3.0
nix run .#e2e -- --controls-only   # start both stacks without compiling the connector
```

The runner starts a private RabbitMQ node and standalone Flink JobManager and TaskManager on loopback.
It runs the Maven tests, verifies that external jobs can use the packaged artifacts, and checks broker redelivery and job recovery across a TaskManager crash.
No Docker daemon or system RabbitMQ, JDK, or Maven is needed.
The first run downloads Nix inputs, the Flink distributions, and Maven dependencies.
The flake exposes `aarch64-darwin` and `x86_64-linux` environments.
Each run prints its log directory under `/tmp` and stops the services at exit; logs and test state are retained for diagnosis.
CI runs the same flake app.
On an uncommitted checkout with new flake files, use `nix run path:.#e2e` so Nix can see the untracked files.

## Developing Flink

The Flink committers use IntelliJ IDEA to develop the Flink codebase.
We recommend IntelliJ IDEA for developing projects that involve Scala code.

Minimal requirements for an IDE are:
* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala

### IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development.

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala Plugin: [https://plugins.jetbrains.com/plugin/?id=1347](https://plugins.jetbrains.com/plugin/?id=1347)

Check out our [Setting up IntelliJ](https://nightlies.apache.org/flink/flink-docs-master/flinkDev/ide_setup.html#intellij-idea) guide for details.

## Support

Don’t hesitate to ask!

Contact the developers and community on the [mailing lists](https://flink.apache.org/community.html#mailing-lists) if you need any help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink.

## Documentation

The documentation of Apache Flink is located on the website: [https://flink.apache.org](https://flink.apache.org)
or in the `docs/` directory of the source code.

## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it.
Contact us if you are looking for implementation tasks that fit your skills.
This article describes [how to contribute to Apache Flink](https://flink.apache.org/contributing/how-to-contribute.html).

## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.
