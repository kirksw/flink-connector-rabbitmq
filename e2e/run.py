# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Start private RabbitMQ and Flink processes and test the deployed artifacts."""

import argparse
from contextlib import ExitStack, contextmanager
import json
import os
import re
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request

import pika


TARGETS = {"1.20.5": "E2E_FLINK_1_20", "2.3.0": "E2E_FLINK_2_3"}
LOOPBACK = "127.0.0.1"


def ports(count):
    with ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(count)]
        for sock in sockets:
            sock.bind((LOOPBACK, 0))
        return [sock.getsockname()[1] for sock in sockets]


@contextmanager
def service(command, env, logfile):
    with logfile.open("wb") as log:
        process = subprocess.Popen(
            command, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
        )
        try:
            yield process
        finally:
            for signum in (signal.SIGTERM, signal.SIGKILL):
                try:
                    os.killpg(process.pid, signum)
                except ProcessLookupError:
                    break
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    continue
            if process.poll() is None:
                raise RuntimeError(f"Failed to stop our process group {process.pid}; see {logfile}")


def await_ready(label, check, *processes):
    deadline = time.monotonic() + 90
    last_error = None
    while time.monotonic() < deadline:
        for process in processes:
            if process.poll() is not None:
                raise RuntimeError(f"Service exited with {process.returncode} waiting for {label}")
        try:
            result = check()
            if result:
                return result
        except (OSError, pika.exceptions.AMQPConnectionError) as error:
            last_error = error
        time.sleep(0.25)
    raise TimeoutError(f"Timed out waiting for {label}: {last_error}")


def exec_logged(command, env, log, timeout=120):
    with service(command, env, log) as process:
        code = process.wait(timeout=timeout)
        if code != 0:
            raise RuntimeError(f"{command[0]} exited with {code}; see {log}")


@contextmanager
def rabbitmq(work):
    amqp, epmd, dist = ports(3)
    broker_config = work / "rabbitmq.conf"
    broker_config.write_text(f"listeners.tcp.1 = {LOOPBACK}:{amqp}\n")
    home = work / "home"
    home.mkdir()
    cookie = home / ".erlang.cookie"
    cookie.write_text(os.urandom(24).hex())
    cookie.chmod(0o400)
    rabbit_env = {**os.environ, "HOME": str(home), "ERL_EPMD_ADDRESS": LOOPBACK,
                  "ERL_EPMD_PORT": str(epmd), "RABBITMQ_NODENAME": f"rabbit_{epmd}@localhost",
                  "RABBITMQ_DIST_PORT": str(dist), "RABBITMQ_CONFIG_FILE": str(broker_config),
                  "RABBITMQ_MNESIA_BASE": str(work / "rabbit-data"), "RABBITMQ_LOG_BASE": str(work),
                  "RABBITMQ_ENABLED_PLUGINS_FILE": str(work / "enabled_plugins"),
                  "ERL_CRASH_DUMP": str(work / "erl_crash.dump"),
                  "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS": "+S 2:2 -kernel inet_dist_use_interface {127,0,0,1}"}
    with ExitStack() as stack:
        epmd_process = stack.enter_context(service(["epmd", "-port", str(epmd)], rabbit_env,
                                                   work / "epmd.log"))
        rabbit = stack.enter_context(service([os.environ["E2E_RABBITMQ"]], rabbit_env,
                                             work / "rabbit.log"))
        params = pika.ConnectionParameters(LOOPBACK, amqp, socket_timeout=1, stack_timeout=3)
        broker = await_ready("AMQP", lambda: pika.BlockingConnection(params), rabbit, epmd_process)
        with broker:
            channel = broker.channel()
            queue = channel.queue_declare(queue="", exclusive=True).method.queue
            channel.confirm_delivery()
            channel.basic_publish("", queue, b"rabbitmq-ok", mandatory=True)
            delivery, _, body = channel.basic_get(queue, auto_ack=True)
            if delivery is None or body != b"rabbitmq-ok":
                raise RuntimeError(f"Unexpected RabbitMQ control result: {body!r}")
        yield amqp


def job_jar(class_name, classpath, root, work, env):
    jar = work / f"{class_name}.jar"
    compiled = work / class_name
    compiled.mkdir(exist_ok=True)
    exec_logged(["javac", "-cp", classpath, "-d", compiled, root / f"e2e/{class_name}.java"],
                env, work / f"{class_name}.javac.log")
    exec_logged(["jar", "cf", jar, "-C", compiled, "."], env, work / f"{class_name}.jar.log")
    return jar


def build_connector(version, work):
    revision = "3.1.0-1.20-SNAPSHOT" if version == "1.20.5" else "3.1.0-2.3-SNAPSHOT"
    profile = [] if version == "1.20.5" else ["-Pflink-2.3"]
    repo = Path(os.environ.get("E2E_MAVEN_REPO", Path.home() / ".m2/repository")).resolve()
    exec_logged(["mvn", "-B", "-ntp", f"-Dmaven.repo.local={repo}", *profile, "clean", "install"],
                os.environ, work / "maven.log", timeout=900)
    thin = repo / f"org/apache/flink/flink-connector-rabbitmq/{revision}/flink-connector-rabbitmq-{revision}.jar"
    client = repo / "com/rabbitmq/amqp-client/5.20.0/amqp-client-5.20.0.jar"
    shaded = repo / f"org/apache/flink/flink-sql-connector-rabbitmq/{revision}/flink-sql-connector-rabbitmq-{revision}.jar"
    return {"thin": [thin, client], "shaded": [shaded]}


def sink_smoke(artifacts, flink, env, amqp, root, work):
    with pika.BlockingConnection(pika.ConnectionParameters(LOOPBACK, amqp)) as broker:
        channel = broker.channel()
        for variant, libraries in artifacts.items():
            queue = f"sink-{variant}"
            classpath = os.pathsep.join(map(str, [flink / "lib/*", *libraries]))
            jar = job_jar("SinkJob", classpath, root, work, env)
            options = [arg for library in libraries for arg in ("-C", library.as_uri())]
            exec_logged([flink / "bin/flink", "run", *options, "-c", "SinkJob", jar, str(amqp), queue],
                        env, work / f"sink-{variant}.log")
            delivery, _, body = channel.basic_get(queue, auto_ack=True)
            if delivery is None or body != b"sink-ok":
                raise RuntimeError(f"Unexpected sink result from {variant} artifact: {body!r}")


def submit(flink, env, class_name, jar, libraries, args, log, restore=None):
    options = [arg for library in libraries for arg in ("-C", library.as_uri())]
    if restore:
        options.extend(["-s", restore])
    exec_logged([flink / "bin/flink", "run", "-d", *options, "-c", class_name, jar, *args], env, log)
    found = re.search(r"JobID ([0-9a-f]{32})", log.read_text())
    if not found:
        raise RuntimeError(f"Could not find submitted job ID in {log}")
    return found.group(1)


def publish(port, queue, records, correlation_ids=True):
    with pika.BlockingConnection(pika.ConnectionParameters(LOOPBACK, port)) as broker:
        channel = broker.channel()
        channel.queue_declare(queue=queue, durable=True)
        channel.confirm_delivery()
        for record in records:
            props = pika.BasicProperties(correlation_id=record if correlation_ids else None)
            channel.basic_publish("", queue, record.encode(), props, mandatory=True)


def routing(artifact, flink, env, amqp, root, work, jm, tm):
    marker = work / "returned-message"
    with pika.BlockingConnection(pika.ConnectionParameters(LOOPBACK, amqp)) as broker:
        channel = broker.channel()
        channel.exchange_declare("route-exchange", "direct")
        channel.queue_declare("routed")
        channel.queue_bind("routed", "route-exchange", "hit")
        publish(amqp, "route-input", ["hit", "miss"], correlation_ids=False)
        classpath = os.pathsep.join(map(str, [flink / "lib/*", *artifact]))
        jar = job_jar("RoutingJob", classpath, root, work, env)
        job_id = submit(flink, env, "RoutingJob", jar, artifact, [str(amqp), marker], work / "routing.log")
        def routed_message():
            result = channel.basic_get("routed", auto_ack=True)
            return result if result[0] is not None else None

        delivery, props, body = await_ready("routed message", routed_message, jm, tm)
        await_ready("unroutable mandatory return", marker.exists, jm, tm)
        if (delivery.exchange, delivery.routing_key, props.correlation_id, props.content_type, body) != (
            "route-exchange", "hit", "routed-hit", "text/plain", b"hit"
        ) or marker.read_text() != "312:miss":
            raise RuntimeError("Unexpected routing, message properties, or mandatory return")
        if channel.queue_declare("route-input", passive=True).method.message_count != 0:
            raise RuntimeError("Non-checkpointed job did not drain its input queue")
    exec_logged([flink / "bin/flink", "cancel", job_id], env, work / "routing-cancel.log")


def flink_rest(rest, path):
    with urllib.request.urlopen(f"http://{LOOPBACK}:{rest}/{path}", timeout=2) as res:
        return json.load(res)


def checkpoint_stats(rest, job_id):
    return flink_rest(rest, f"jobs/{job_id}/checkpoints")


def committed_counts(scenario, ids, jm, tm, after=0):
    def report():
        results = [file for file in scenario.glob("counts-*") if int(file.name.split("-")[1]) > after]
        if not results:
            return None
        latest = max(results, key=lambda file: int(file.name.split("-")[1]))
        counts = dict(line.split("=", 1) for line in latest.read_text().splitlines())
        return counts if ids.issubset(counts) else None

    return await_ready(f"{scenario.name} checkpoint-committed result", report, jm, tm)


def recovery(artifact, flink, env, amqp, root, work, rest, jm, tm, start_tm, mode, parallelism):
    scenario = work / f"{mode.lower()}-{parallelism}"
    scenario.mkdir()
    queue = scenario.name
    ids = {"1", "2", "3", "4"}
    publish(amqp, queue, sorted(ids), correlation_ids=mode == "EXACTLY_ONCE")
    classpath = os.pathsep.join(map(str, [flink / "lib/*", *artifact]))
    jar = job_jar("RecoveryJob", classpath, root, scenario, env)
    args = [str(amqp), queue, mode, str(parallelism), scenario]
    job_id = submit(flink, env, "RecoveryJob", jar, artifact, args, scenario / "submit.log")
    await_ready(f"{queue} checkpoint before RabbitMQ commit", (scenario / "before-commit").exists, jm, tm)
    completed = checkpoint_stats(rest, job_id)["latest"]["completed"]["id"]
    os.killpg(tm.pid, signal.SIGKILL)
    tm.wait(timeout=8)
    tm = start_tm()
    await_ready(f"{queue} broker redelivery", lambda: list(scenario.glob("redelivered-*")), jm, tm)
    publish(amqp, queue, ["report"])
    counts = committed_counts(scenario, ids, jm, tm)
    if set(counts) != ids or (mode == "EXACTLY_ONCE" and any(v != "1" for v in counts.values())):
        raise RuntimeError(f"Unexpected recovery result in {queue}: {counts}")
    if mode == "AT_LEAST_ONCE" and not any(int(v) > 1 for v in counts.values()):
        raise RuntimeError(f"At-least-once test did not expose a duplicate: {counts}")
    print(f"{queue}: job {job_id}, checkpoint {completed}, counts {counts}", flush=True)
    if mode == "EXACTLY_ONCE":
        savepoint_directory = scenario / "savepoints"
        savepoint_directory.mkdir()
        log = scenario / "savepoint.log"
        exec_logged([flink / "bin/flink", "stop", "-p", savepoint_directory.as_uri(), job_id], env, log)
        savepoint = re.search(r"Path: (\S+)", log.read_text())
        if not savepoint:
            raise RuntimeError(f"No savepoint path in {log}")
        for file in scenario.glob("counts-*"):
            file.rename(file.with_name(f"original-{file.name}"))
        job_id = submit(flink, env, "RecoveryJob", jar, artifact, args, scenario / "restore.log",
                        restore=savepoint.group(1))
        publish(amqp, queue, ["5", "final-report"])
        ids.add("5")
        restored = committed_counts(scenario, ids, jm, tm)
        if restored != {identifier: "1" for identifier in ids}:
            raise RuntimeError(f"Unexpected savepoint restore counts: {restored}")
        print(f"{queue}: same-version savepoint restore {restored}", flush=True)
    exec_logged([flink / "bin/flink", "cancel", job_id], env, scenario / "cancel.log")
    return tm


def checkpoint_abort(artifact, flink, env, amqp, root, work, rest, jm, tm):
    scenario = work / "aborted-checkpoint"
    scenario.mkdir()
    (scenario / "abort-once").touch()
    (scenario / "allow-commit").touch()
    queue = scenario.name
    publish(amqp, queue, ["1", "2", "report"])
    jar = job_jar("RecoveryJob", os.pathsep.join(map(str, [flink / "lib/*", *artifact])), root, scenario, env)
    job_id = submit(flink, env, "RecoveryJob", jar, artifact,
                    [str(amqp), queue, "EXACTLY_ONCE", "1", scenario], scenario / "submit.log")
    marker = scenario / "held-checkpoint"
    held = int(await_ready("held checkpoint", lambda: marker.read_text() if marker.exists() else None, jm, tm))

    def failed_checkpoint():
        failed = checkpoint_stats(rest, job_id)["latest"]["failed"]
        return failed and failed["id"] == held

    await_ready(f"aborted checkpoint {held}", failed_checkpoint, jm, tm)
    if (scenario / "before-commit").exists():
        raise RuntimeError("RabbitMQ acknowledgement ran before a checkpoint completed")
    (scenario / "release-checkpoint").touch()
    await_ready("broker commit in a later checkpoint", (scenario / "committed").exists, jm, tm)
    counts = committed_counts(scenario, {"1", "2"}, jm, tm, after=held)
    if counts != {"1": "1", "2": "1"}:
        raise RuntimeError(f"Unexpected post-abort counts: {counts}")
    exec_logged([flink / "bin/flink", "cancel", job_id], env, scenario / "cancel.log")
    print(f"job {job_id}: checkpoint {held} aborted without acknowledging deliveries", flush=True)


def missing_correlation(artifact, flink, env, amqp, root, work, rest, jm, tm):
    scenario = work / "missing-correlation"
    scenario.mkdir()
    queue = scenario.name
    publish(amqp, queue, ["1"], correlation_ids=False)
    jar = job_jar("RecoveryJob", os.pathsep.join(map(str, [flink / "lib/*", *artifact])), root, scenario, env)
    job_id = submit(flink, env, "RecoveryJob", jar, artifact,
                    [str(amqp), queue, "EXACTLY_ONCE", "1", scenario, "fail-fast"], scenario / "submit.log")
    await_ready("job to reject missing correlation ID", lambda: flink_rest(rest, f"jobs/{job_id}")["state"]
                == "FAILED", jm, tm)
    errors = flink_rest(rest, f"jobs/{job_id}/exceptions")
    if "requires a unique correlation ID" not in json.dumps(errors):
        raise RuntimeError(f"Job {job_id} failed for the wrong reason: {errors}")
    with pika.BlockingConnection(pika.ConnectionParameters(LOOPBACK, amqp)) as broker:
        channel = broker.channel()

        def recovered_delivery():
            delivery, _, value = channel.basic_get(queue, auto_ack=False)
            return (delivery, value) if delivery is not None else None

        delivery, value = await_ready("invalid message to requeue", recovered_delivery, jm, tm)
        if value != b"1" or not delivery.redelivered:
            raise RuntimeError("Missing-correlation delivery was not requeued")
    print(f"job {job_id}: rejected missing correlation ID without acknowledging it", flush=True)


def run(version, root, controls_only):
    # A short directory name avoids limits on Erlang and local IPC paths.
    work = Path(tempfile.mkdtemp(prefix="rmq-e2e-", dir="/tmp")).resolve()
    print(f"Flink {version}: logs and results in {work}", flush=True)
    artifacts = None if controls_only else build_connector(version, work)
    rest, rpc = ports(2)
    flink = Path(os.environ[TARGETS[version]])
    conf = work / "conf"
    conf.mkdir()
    for config in (flink / "conf").glob("log*"):
        (conf / config.name).write_bytes(config.read_bytes())
    defaults = (flink / "conf" / "config.yaml").read_text()
    java_opts = next((line.strip() for line in defaults.splitlines()
                      if line.strip().startswith("all: --add-")), None)
    config = f"""\
jobmanager.rpc.address: {LOOPBACK}
jobmanager.rpc.port: {rpc}
jobmanager.bind-host: {LOOPBACK}
jobmanager.memory.process.size: 1024m
rest.address: {LOOPBACK}
rest.bind-address: {LOOPBACK}
rest.port: {rest}
heartbeat.interval: 1000
heartbeat.timeout: 8000
restart-strategy.type: fixed-delay
restart-strategy.fixed-delay.attempts: 10
restart-strategy.fixed-delay.delay: 1s
jobmanager.execution.failover-strategy: full
taskmanager.host: {LOOPBACK}
taskmanager.bind-host: {LOOPBACK}
taskmanager.memory.process.size: 1024m
taskmanager.numberOfTaskSlots: 2
parallelism.default: 1
state.checkpoints.dir: {(work / 'checkpoints').as_uri()}
execution.checkpointing.dir: {(work / 'checkpoints').as_uri()}
env.log.dir: {work}
env.pid.dir: {work}
io.tmp.dirs: {work}
"""
    if java_opts:
        config += "env:\n  java:\n    default-opts:\n      " + java_opts + "\n"
    (conf / "config.yaml").write_text(config)
    flink_env = {**os.environ, "FLINK_CONF_DIR": str(conf)}
    with ExitStack() as stack:
        amqp = stack.enter_context(rabbitmq(work))
        jm = stack.enter_context(service([flink / "bin/jobmanager.sh", "start-foreground"],
                                         flink_env, work / "jobmanager.log"))
        starts = iter(range(20))

        def start_tm():
            return stack.enter_context(service([flink / "bin/taskmanager.sh", "start-foreground"],
                                               flink_env, work / f"taskmanager-{next(starts)}.log"))

        tm = start_tm()

        def slots():
            with urllib.request.urlopen(f"http://{LOOPBACK}:{rest}/overview", timeout=1) as res:
                return json.load(res)["slots-available"]

        await_ready("Flink task slots", slots, jm, tm)
        jar = job_jar("ControlJob", str(flink / "lib/*"), root, work, flink_env)
        marker = work / "control.result"
        exec_logged([flink / "bin/flink", "run", "-c", "ControlJob", jar, marker],
                    flink_env, work / "submission.log")
        if marker.read_text() != "flink-ok":
            raise RuntimeError("Flink control job did not produce the expected result")
        if artifacts:
            sink_smoke(artifacts, flink, flink_env, amqp, root, work)
            routing(artifacts["thin"], flink, flink_env, amqp, root, work, jm, tm)
            missing_correlation(artifacts["thin"], flink, flink_env, amqp, root, work, rest, jm, tm)
            checkpoint_abort(artifacts["thin"], flink, flink_env, amqp, root, work, rest, jm, tm)
            for mode, parallelism in [("EXACTLY_ONCE", 1), ("AT_LEAST_ONCE", 1), ("AT_LEAST_ONCE", 2)]:
                tm = recovery(artifacts["thin"], flink, flink_env, amqp, root, work,
                              rest, jm, tm, start_tm, mode, parallelism)
    checks = "standalone job and AMQP controls" if controls_only else "packaged sink and checkpoint recovery jobs"
    print(f"Flink {version}: {checks} passed", flush=True)


def interrupted(signum, _frame):
    raise SystemExit(128 + signum)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flink", choices=TARGETS, help="run only the selected version")
    parser.add_argument("--controls-only", action="store_true", help="skip building the connector")
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, interrupted)
    root = Path.cwd()
    if not (root / "e2e/ControlJob.java").is_file():
        parser.error("run from the connector repository root")
    for version in [args.flink] if args.flink else TARGETS:
        run(version, root, args.controls_only)


if __name__ == "__main__":
    main()
