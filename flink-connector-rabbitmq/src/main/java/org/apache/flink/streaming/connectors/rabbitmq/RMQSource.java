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

package org.apache.flink.streaming.connectors.rabbitmq;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.UserCodeClassLoader;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/** An unbounded RabbitMQ queue source. Use {@link #addTo} to validate checkpoint settings. */
@PublicEvolving
public class RMQSource<OUT>
        implements Source<OUT, RMQSource.Split, List<RMQSource.Split>>, ResultTypeQueryable<OUT> {
    private static final long serialVersionUID = 1L;

    /** Determines when RabbitMQ may remove deliveries from its queue. */
    @PublicEvolving
    public enum AcknowledgementMode {
        /** The broker removes messages when delivered, independently of Flink. */
        AUTO_ACK,
        /** Acknowledge completed checkpoints. Checkpointing must be enabled. */
        AT_LEAST_ONCE,
        /** Checkpoint and deduplicate unique producer correlation IDs at parallelism one. */
        EXACTLY_ONCE
    }

    private final RMQConnectionConfig config;
    private final String queueName;
    private final AcknowledgementMode mode;
    private final RMQDeserializationSchema<OUT> schema;

    public RMQSource(
            RMQConnectionConfig config,
            String queueName,
            AcknowledgementMode mode,
            DeserializationSchema<OUT> schema) {
        this(config, queueName, mode, new RMQDeserializationSchemaWrapper<>(schema));
    }

    public RMQSource(
            RMQConnectionConfig config,
            String queueName,
            AcknowledgementMode mode,
            RMQDeserializationSchema<OUT> schema) {
        this.config = Objects.requireNonNull(config);
        this.queueName = Objects.requireNonNull(queueName);
        this.mode = Objects.requireNonNull(mode);
        this.schema = Objects.requireNonNull(schema);
    }

    /**
     * Adds this source after validating the environment's checkpoint settings. Configure
     * checkpointing before calling this method and do not disable or weaken it afterwards.
     *
     * @throws IllegalArgumentException if the environment does not meet the acknowledgement mode's
     *     checkpoint requirements
     */
    public final DataStreamSource<OUT> addTo(
            StreamExecutionEnvironment env, WatermarkStrategy<OUT> watermarks, String name) {
        CheckpointConfig checkpoints = env.getCheckpointConfig();
        if (mode != AcknowledgementMode.AUTO_ACK && !checkpoints.isCheckpointingEnabled()) {
            throw new IllegalArgumentException("RabbitMQ " + mode + " requires Flink checkpointing");
        }
        if (mode == AcknowledgementMode.EXACTLY_ONCE
                && checkpoints.getCheckpointingConsistencyMode() != CheckpointingMode.EXACTLY_ONCE) {
            throw new IllegalArgumentException(
                    "RabbitMQ EXACTLY_ONCE requires Flink EXACTLY_ONCE checkpoint mode");
        }
        return env.fromSource(this, watermarks, name);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return schema.getProducedType();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<OUT, Split> createReader(SourceReaderContext context) {
        return new Reader(context);
    }

    @Override
    public SplitEnumerator<Split, List<Split>> createEnumerator(
            SplitEnumeratorContext<Split> context) {
        List<Split> splits = new ArrayList<>();
        for (int index = 0; index < context.currentParallelism(); index++) {
            splits.add(new Split(index, Set.of(), false));
        }
        return restoreEnumerator(context, splits);
    }

    @Override
    public SplitEnumerator<Split, List<Split>> restoreEnumerator(
            SplitEnumeratorContext<Split> context, List<Split> checkpoint) {
        if (mode == AcknowledgementMode.EXACTLY_ONCE && context.currentParallelism() != 1) {
            throw new IllegalArgumentException("RabbitMQ exactly-once mode requires parallelism 1");
        }
        return new Enumerator(context, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<Split> getSplitSerializer() {
        return new SplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<List<Split>> getEnumeratorCheckpointSerializer() {
        return new EnumeratorSerializer();
    }

    /** Configure the RabbitMQ queue or bind it to an exchange before consuming. */
    protected void setupQueue(Channel channel, String queue) throws IOException {
        Util.declareQueueDefaults(channel, queue);
    }

    /** Opens a channel session; Flink, not the client, must recover it after connection loss. */
    protected Connection setupConnection() throws Exception {
        ConnectionFactory factory = config.getConnectionFactory();
        factory.setAutomaticRecoveryEnabled(false);
        return factory.newConnection();
    }

    /** One logical consumer assignment, not a disjoint partition or RabbitMQ offset. */
    @PublicEvolving
    public static final class Split implements SourceSplit {
        private final int index;
        private final Set<String> ids;
        private final boolean finished;

        Split(int index, Set<String> ids, boolean finished) {
            this.index = index;
            this.ids = Set.copyOf(ids);
            this.finished = finished;
        }

        @Override
        public String splitId() {
            return Integer.toString(index);
        }
    }

    private static final class Enumerator implements SplitEnumerator<Split, List<Split>> {
        private final SplitEnumeratorContext<Split> context;
        private final Queue<Split> unassigned;

        Enumerator(SplitEnumeratorContext<Split> context, Collection<Split> pending) {
            this.context = context;
            this.unassigned = new ArrayDeque<>(pending);
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, String hostname) {
            Split split = unassigned.poll();
            if (split != null) {
                context.assignSplit(split, subtaskId);
            }
            context.signalNoMoreSplits(subtaskId);
        }

        @Override
        public void addSplitsBack(List<Split> splits, int subtaskId) {
            unassigned.addAll(splits);
        }

        @Override
        public void addReader(int subtaskId) {}

        @Override
        public List<Split> snapshotState(long checkpointId) {
            return List.copyOf(unassigned);
        }

        @Override
        public void close() {}
    }

    private final class Reader implements SourceReader<OUT, Split> {
        private final SourceReaderContext context;
        private final Object deliveryLock = new Object();
        private final Queue<Delivery> deliveries = new ArrayDeque<>();
        // A restored ID starts with zero pending deliveries on the new channel.
        private final Map<String, Integer> pendingById = new HashMap<>();
        private final List<PendingAck> unsnapshotted = new ArrayList<>();
        private final NavigableMap<Long, List<PendingAck>> checkpoints = new TreeMap<>();
        private CompletableFuture<Void> available = new CompletableFuture<>();
        private Throwable deliveryError;
        private Split split;
        private boolean started;
        private boolean noMoreSplits;
        private volatile boolean finished;
        private Connection connection;
        private Channel channel;

        Reader(SourceReaderContext context) {
            this.context = context;
        }

        @Override
        public void start() {
            started = true;
            if (split == null) {
                context.sendSplitRequest();
            } else {
                openConsumer();
            }
        }

        @Override
        public void addSplits(List<Split> splits) {
            if (splits.size() != 1 || split != null) {
                throw new IllegalArgumentException("RabbitMQ requires one split per reader");
            }
            split = splits.get(0);
            split.ids.forEach(id -> pendingById.put(id, 0));
            finished = split.finished;
            if (started) {
                openConsumer();
            }
        }

        private void openConsumer() {
            if (finished) {
                signalAvailable();
                return;
            }
            try {
                schema.open(new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return context.metricGroup().addGroup("user");
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return context.getUserCodeClassLoader();
                    }
                });
                connection = setupConnection();
                channel = connection.createChannel();
                if (channel == null) {
                    throw new IOException("RabbitMQ has no available channels");
                }
                setupQueue(channel, queueName);
                channel.basicQos(config.getPrefetchCount().orElse(1000));
                boolean autoAck = mode == AcknowledgementMode.AUTO_ACK;
                if (!autoAck) {
                    channel.txSelect();
                }
                channel.basicConsume(queueName, autoAck, new Consumer(channel));
            } catch (Exception error) {
                try {
                    close();
                } catch (Exception closeError) {
                    error.addSuppressed(closeError);
                }
                throw new IllegalStateException("Could not start RabbitMQ consumer for " + queueName, error);
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<OUT> output) throws IOException {
            Delivery delivery;
            synchronized (deliveryLock) {
                if (deliveryError != null) {
                    throw new IOException("RabbitMQ consumer disconnected", deliveryError);
                }
                if (split == null && noMoreSplits) {
                    return InputStatus.END_OF_INPUT;
                }
                delivery = finished ? null : deliveries.poll();
            }
            if (delivery != null) {
                processDelivery(delivery, output);
            }
            synchronized (deliveryLock) {
                if (finished && (mode == AcknowledgementMode.AUTO_ACK
                        || (unsnapshotted.isEmpty() && checkpoints.isEmpty()))) {
                    return InputStatus.END_OF_INPUT;
                }
                if (deliveryError != null || (!finished && !deliveries.isEmpty())) {
                    return InputStatus.MORE_AVAILABLE;
                }
                if (available.isDone()) {
                    available = new CompletableFuture<>();
                }
                return InputStatus.NOTHING_AVAILABLE;
            }
        }

        private void processDelivery(Delivery delivery, ReaderOutput<OUT> output) throws IOException {
            Envelope envelope = delivery.getEnvelope();
            AMQP.BasicProperties props = delivery.getProperties();
            DeliveryCollector collector = new DeliveryCollector(props.getCorrelationId(), envelope.getDeliveryTag());
            schema.deserialize(envelope, props, delivery.getBody(), collector);
            String id = collector.id;
            boolean emit = true;
            if (mode == AcknowledgementMode.EXACTLY_ONCE) {
                if (id == null || id.isEmpty()) {
                    throw new IOException("RabbitMQ exactly-once mode requires a unique correlation ID");
                }
                Integer pending = pendingById.get(id);
                emit = pending == null;
                pendingById.put(id, emit ? 1 : pending + 1);
            }
            if (emit) {
                for (OUT record : collector.records) {
                    if (schema.isEndOfStream(record)) {
                        finished = true;
                        break;
                    }
                    output.collect(record);
                }
            }
            if (mode != AcknowledgementMode.AUTO_ACK) {
                unsnapshotted.add(new PendingAck(envelope.getDeliveryTag(), id));
            }
        }

        @Override
        public List<Split> snapshotState(long checkpointId) {
            if (split == null) {
                return List.of();
            }
            if (!unsnapshotted.isEmpty()) {
                checkpoints.put(checkpointId, new ArrayList<>(unsnapshotted));
                unsnapshotted.clear();
            }
            return List.of(new Split(split.index, pendingById.keySet(), finished));
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws IOException {
            // A later completed checkpoint also covers deliveries from earlier aborted checkpoints.
            NavigableMap<Long, List<PendingAck>> ready = checkpoints.headMap(checkpointId, true);
            if (ready.isEmpty()) {
                return;
            }
            for (List<PendingAck> batch : ready.values()) {
                for (PendingAck ack : batch) {
                    channel.basicAck(ack.tag, false);
                }
            }
            channel.txCommit();
            if (mode == AcknowledgementMode.EXACTLY_ONCE) {
                for (List<PendingAck> batch : ready.values()) {
                    for (PendingAck ack : batch) {
                        pendingById.compute(
                                ack.id, (id, pending) -> pending == 1 ? null : pending - 1);
                    }
                }
            }
            ready.clear();
            if (finished) {
                signalAvailable();
            }
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            synchronized (deliveryLock) {
                return available;
            }
        }

        @Override
        public void notifyNoMoreSplits() {
            noMoreSplits = true;
            signalAvailable();
        }

        @Override
        public void close() throws Exception {
            try (Connection openConnection = connection; Channel openChannel = channel) {
                // Close the channel before the connection so unacknowledged deliveries are requeued.
            }
        }

        private void signalAvailable() {
            synchronized (deliveryLock) {
                available.complete(null);
            }
        }

        private final class Consumer extends DefaultConsumer {
            Consumer(Channel channel) {
                super(channel);
            }

            @Override
            public void handleDelivery(
                    String tag, Envelope envelope, AMQP.BasicProperties props, byte[] body) {
                synchronized (deliveryLock) {
                    if (!finished) {
                        deliveries.add(new Delivery(envelope, props, body));
                        available.complete(null);
                    }
                }
            }

            @Override
            public void handleCancel(String tag) {
                fail(new IOException("RabbitMQ cancelled consumer " + tag));
            }

            @Override
            public void handleShutdownSignal(String tag, ShutdownSignalException signal) {
                fail(signal);
            }

            private void fail(Throwable error) {
                synchronized (deliveryLock) {
                    deliveryError = error;
                    available.complete(null);
                }
            }
        }

        private final class DeliveryCollector implements RMQDeserializationSchema.RMQCollector<OUT> {
            private final List<OUT> records = new ArrayList<>();
            private final long tag;
            private String id;
            private boolean customIdSet;

            DeliveryCollector(String id, long tag) {
                this.id = id;
                this.tag = tag;
            }

            @Override
            public boolean setMessageIdentifiers(String correlationId, long deliveryTag) {
                if (customIdSet || !records.isEmpty() || tag != deliveryTag) {
                    throw new IllegalArgumentException(
                            "Set the correlation ID once, before collecting, using the original delivery tag");
                }
                customIdSet = true;
                id = correlationId;
                return mode != AcknowledgementMode.EXACTLY_ONCE || !pendingById.containsKey(id);
            }

            @Override
            public void collect(OUT record) {
                records.add(record);
            }

            @Override
            public void close() {}
        }
    }

    private static final class PendingAck {
        private final long tag;
        private final String id;

        PendingAck(long tag, String id) {
            this.tag = tag;
            this.id = id;
        }
    }

    private static final class SplitSerializer implements SimpleVersionedSerializer<Split> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Split split) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                writeSplit(out, split);
            }
            return bytes.toByteArray();
        }

        @Override
        public Split deserialize(int version, byte[] serialized) throws IOException {
            checkVersion(version);
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
                return readSplit(in);
            }
        }
    }

    private static final class EnumeratorSerializer implements SimpleVersionedSerializer<List<Split>> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(List<Split> splits) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeInt(splits.size());
                for (Split split : splits) {
                    writeSplit(out, split);
                }
            }
            return bytes.toByteArray();
        }

        @Override
        public List<Split> deserialize(int version, byte[] serialized) throws IOException {
            checkVersion(version);
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
                int size = in.readInt();
                List<Split> splits = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    splits.add(readSplit(in));
                }
                return splits;
            }
        }
    }

    private static void checkVersion(int version) throws IOException {
        if (version != 1) {
            throw new IOException("Unsupported RabbitMQ checkpoint version " + version);
        }
    }

    private static void writeSplit(DataOutputStream out, Split split) throws IOException {
        out.writeInt(split.index);
        out.writeBoolean(split.finished);
        out.writeInt(split.ids.size());
        for (String id : split.ids) {
            out.writeUTF(id);
        }
    }

    private static Split readSplit(DataInputStream in) throws IOException {
        int index = in.readInt();
        boolean finished = in.readBoolean();
        int size = in.readInt();
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < size; i++) {
            ids.add(in.readUTF());
        }
        return new Split(index, ids, finished);
    }
}
