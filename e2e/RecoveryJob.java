/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource.AcknowledgementMode;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/** Job that verifies output recovered from the last completed Flink checkpoint. */
public final class RecoveryJob {
    private static final String REPORT = "report";

    private static boolean isReport(String value) {
        return REPORT.equals(value) || "final-report".equals(value);
    }

    public static void main(String[] args) throws Exception {
        RMQConnectionConfig config = new RMQConnectionConfig.Builder()
                .setUri("amqp://guest:guest@127.0.0.1:" + args[0])
                .setPrefetchCount("2".equals(args[3]) ? 2 : 16)
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        if (args.length == 6 && "fail-fast".equals(args[5])) {
            env.configure(Configuration.fromMap(Map.of("restart-strategy.type", "none")));
        }
        if (Files.exists(Path.of(args[4], "abort-once"))) {
            env.getCheckpointConfig().setCheckpointTimeout(3000);
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        }
        RMQSource<String> source = new HeldCommitSource(
                config, args[1], AcknowledgementMode.valueOf(args[2]), args[4]);
        source.addTo(env, WatermarkStrategy.noWatermarks(), "rabbitmq")
                .uid("rabbitmq")
                .setParallelism(Integer.parseInt(args[3]))
                .map(new CountingOperator(args[4]))
                .uid("counts")
                .setParallelism(1);
        env.execute("rabbitmq-recovery-" + args[2]);
    }

    private static final class HeldCommitSource extends RMQSource<String> {
        private final String work;

        HeldCommitSource(RMQConnectionConfig config, String queue, AcknowledgementMode mode,
                         String work) {
            super(config, queue, mode, new TrackingSchema(work));
            this.work = work;
        }

        @Override
        protected Connection setupConnection() throws Exception {
            Connection real = super.setupConnection();
            return (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                    (proxy, method, args) -> {
                        try {
                            Object result = method.invoke(real, args);
                            if (!(result instanceof Channel)) {
                                return result;
                            }
                            return holdFirstCommit((Channel) result, work);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                    });
        }
    }

    private static Channel holdFirstCommit(Channel real, String work) {
        return (Channel) Proxy.newProxyInstance(Channel.class.getClassLoader(),
                new Class<?>[] {Channel.class}, (proxy, method, args) -> {
                    if (method.getName().equals("txCommit")) {
                        Path held = Path.of(work, "before-commit");
                        boolean first;
                        try {
                            Files.createFile(held);
                            first = true;
                        } catch (FileAlreadyExistsException restart) {
                            first = false;
                        }
                        if (first && !Files.exists(Path.of(work, "allow-commit"))) {
                            // The harness kills this TaskManager without allowing the commit.
                            Thread.sleep(90_000);
                            throw new IOException("Timed out waiting for TaskManager failure");
                        }
                    }
                    try {
                        Object result = method.invoke(real, args);
                        if (method.getName().equals("txCommit")) {
                            Files.writeString(Path.of(work, "committed"), "yes");
                        }
                        return result;
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static final class TrackingSchema implements RMQDeserializationSchema<String> {
        private final String work;

        TrackingSchema(String work) {
            this.work = work;
        }

        @Override
        public void deserialize(Envelope envelope, AMQP.BasicProperties props, byte[] payload,
                                RMQCollector<String> out) throws IOException {
            String record = new String(payload, StandardCharsets.UTF_8);
            if (envelope.isRedeliver() && !isReport(record)) {
                Files.writeString(Path.of(work, "redelivered-" + record), "yes");
            }
            out.collect(record);
        }

        @Override
        public boolean isEndOfStream(String record) {
            return false;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    private static final class CountingOperator extends RichMapFunction<String, String>
            implements CheckpointedFunction, CheckpointListener {
        private final String work;
        private final Map<String, Integer> counts = new TreeMap<>();
        private transient ListState<String> state;
        private transient Map<Long, String> reports;
        private boolean reportRequested;

        CountingOperator(String work) {
            this.work = work;
        }

        @Override
        public String map(String value) {
            if (isReport(value)) {
                reportRequested = true;
            } else {
                counts.merge(value, 1, Integer::sum);
            }
            return value;
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            state = ctx.getOperatorStateStore().getListState(new ListStateDescriptor<>("counts", Types.STRING));
            for (String entry : state.get()) {
                String[] parts = entry.split("=", 2);
                counts.put(parts[0], Integer.parseInt(parts[1]));
            }
            reports = new TreeMap<>();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            Path requestingAbort = Path.of(work, "abort-once");
            if (Files.exists(requestingAbort) && !counts.isEmpty()) {
                Files.delete(requestingAbort);
                Files.writeString(Path.of(work, "held-checkpoint"), Long.toString(ctx.getCheckpointId()));
                long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
                while (!Files.exists(Path.of(work, "release-checkpoint"))) {
                    if (System.nanoTime() >= deadline) {
                        throw new IOException("Timeout waiting to release held checkpoint");
                    }
                    Thread.sleep(50);
                }
            }
            state.clear();
            StringBuilder result = new StringBuilder();
            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                String count = entry.getKey() + "=" + entry.getValue();
                state.add(count);
                result.append(count).append('\n');
            }
            if (reportRequested) {
                reports.put(ctx.getCheckpointId(), result.toString());
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpoint) throws IOException {
            String report = reports.remove(checkpoint);
            if (report != null) {
                Path writing = Path.of(work, "counts.tmp");
                Files.writeString(writing, report);
                Files.move(writing, Path.of(work, "counts-" + checkpoint),
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            }
        }
    }
}
