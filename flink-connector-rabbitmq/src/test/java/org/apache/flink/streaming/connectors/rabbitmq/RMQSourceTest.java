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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource.AcknowledgementMode;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource.Split;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RMQSourceTest {
    private Connection connection;
    private Channel channel;
    private ReaderOutput<String> output;
    private Consumer consumer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() throws Exception {
        connection = mock(Connection.class);
        channel = mock(Channel.class);
        output = mock(ReaderOutput.class);
        when(connection.createChannel()).thenReturn(channel);
    }

    @Test
    void failedCheckpointAcknowledgementsAreSubsumedByLaterCompletion() throws Exception {
        try (SourceReader<String, Split> reader = reader(AcknowledgementMode.AT_LEAST_ONCE, emptySplit())) {
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
            assertThat(reader.isAvailable()).isNotDone();
            deliver(1, null, false);
            assertThat(reader.isAvailable()).isDone();
            reader.pollNext(output);
            reader.snapshotState(10);
            reader.notifyCheckpointAborted(10);
            verify(channel, never()).basicAck(1, false);
            deliver(2, null, false);
            reader.pollNext(output);
            reader.snapshotState(11);
            deliver(3, null, false);
            reader.pollNext(output);
            reader.notifyCheckpointComplete(11);
            verify(channel).basicAck(1, false);
            verify(channel).basicAck(2, false);
            verify(channel, never()).basicAck(3, false);
            verify(channel).txCommit();
        }
    }

    @Test
    void restoredCorrelationIdSuppressesRedeliveryAndUsesNewChannelTag() throws Exception {
        SimpleVersionedSerializer<Split> serializer = source(AcknowledgementMode.EXACTLY_ONCE).getSplitSerializer();
        byte[] state;
        try (SourceReader<String, Split> reader = reader(AcknowledgementMode.EXACTLY_ONCE, emptySplit())) {
            deliver(5, "id", false);
            reader.pollNext(output);
            state = serializer.serialize(reader.snapshotState(1).get(0));
            verify(output).collect("test-body");
            verify(channel, never()).basicAck(5, false);
        }
        setup();
        try (SourceReader<String, Split> reader = reader(AcknowledgementMode.EXACTLY_ONCE,
                serializer.deserialize(serializer.getVersion(), state))) {
            deliver(1, "id", true);
            reader.pollNext(output);
            assertThat(reader.snapshotState(2)).hasSize(1);
            reader.notifyCheckpointComplete(2);
            verify(output, never()).collect("test-body");
            verify(channel, never()).basicAck(5, false);
            verify(channel).basicAck(1, false);
            verify(channel).txCommit();
        }
    }

    @Test
    void retainsCorrelationIdUntilLaterDeliveryIsCommitted() throws Exception {
        SimpleVersionedSerializer<Split> serializer =
                source(AcknowledgementMode.EXACTLY_ONCE).getSplitSerializer();
        byte[] state;
        try (SourceReader<String, Split> reader =
                reader(AcknowledgementMode.EXACTLY_ONCE, emptySplit())) {
            deliver(1, "id", false);
            reader.pollNext(output);
            reader.snapshotState(1);
            deliver(2, "id", false);
            reader.pollNext(output);
            reader.notifyCheckpointComplete(1);
            verify(output).collect("test-body");
            verify(channel).basicAck(1, false);
            verify(channel, never()).basicAck(2, false);
            state = serializer.serialize(reader.snapshotState(2).get(0));
        }

        setup();
        try (SourceReader<String, Split> reader = reader(AcknowledgementMode.EXACTLY_ONCE,
                serializer.deserialize(serializer.getVersion(), state))) {
            deliver(1, "id", true);
            reader.pollNext(output);
            verify(output, never()).collect("test-body");
            reader.snapshotState(3);
            reader.notifyCheckpointComplete(3);
            verify(channel).basicAck(1, false);
            assertThat(serializer.serialize(reader.snapshotState(4).get(0)))
                    .containsExactly(serializer.serialize(emptySplit()));
        }
    }

    @Test
    void rejectsMissingCorrelationIdWithoutAcknowledging() throws Exception {
        try (SourceReader<String, Split> reader = reader(AcknowledgementMode.EXACTLY_ONCE, emptySplit())) {
            deliver(1, null, false);
            assertThatThrownBy(() -> reader.pollNext(output)).isInstanceOf(IOException.class)
                    .hasMessageContaining("correlation ID");
            verify(output, never()).collect("test-body");
            verify(channel, never()).basicAck(1, false);
        }
    }

    @Test
    void autoAcknowledgementDoesNotUseTransactionsOrManualAcknowledgements() throws Exception {
        try (SourceReader<String, Split> reader = reader(AcknowledgementMode.AUTO_ACK, emptySplit())) {
            deliver(1, null, false);
            reader.pollNext(output);
            reader.snapshotState(1);
            reader.notifyCheckpointComplete(1);
            verify(output).collect("test-body");
            verify(channel, never()).basicAck(1, false);
            verify(channel, never()).txSelect();
            verify(channel, never()).txCommit();
        }
    }

    @ParameterizedTest
    @EnumSource(value = AcknowledgementMode.class, names = {"AT_LEAST_ONCE", "EXACTLY_ONCE"})
    void checkpointedModeRejectsDisabledCheckpointing(AcknowledgementMode mode) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1)) {
            assertThatThrownBy(() -> source(mode).addTo(env, WatermarkStrategy.noWatermarks(), "rmq"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("requires Flink checkpointing");
        }
    }

    @Test
    void exactlyOnceRequiresMatchingFlinkCheckpointMode() throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1)) {
            env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
            assertThat(source(AcknowledgementMode.AT_LEAST_ONCE)
                            .addTo(env, WatermarkStrategy.noWatermarks(), "at-least-once"))
                    .isNotNull();
            assertThatThrownBy(() -> source(AcknowledgementMode.EXACTLY_ONCE)
                            .addTo(env, WatermarkStrategy.noWatermarks(), "exactly-once"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("requires Flink EXACTLY_ONCE checkpoint mode");
            env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
            assertThat(source(AcknowledgementMode.EXACTLY_ONCE)
                            .addTo(env, WatermarkStrategy.noWatermarks(), "exactly-once"))
                    .isNotNull();
        }
    }

    @Test
    void autoAckDoesNotRequireCheckpoints() throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1)) {
            assertThat(source(AcknowledgementMode.AUTO_ACK)
                            .addTo(env, WatermarkStrategy.noWatermarks(), "auto-ack"))
                    .isNotNull();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void exactlyOnceRequiresSingleReader() {
        SplitEnumeratorContext<Split> context = mock(SplitEnumeratorContext.class);
        when(context.currentParallelism()).thenReturn(2);
        assertThatThrownBy(() -> source(AcknowledgementMode.EXACTLY_ONCE).createEnumerator(context))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("parallelism 1");
    }

    private RMQSource<String> source(AcknowledgementMode mode) {
        return new RMQSource<String>(mock(RMQConnectionConfig.class), "input", mode, new SimpleStringSchema()) {
            @Override
            protected Connection setupConnection() {
                return connection;
            }
        };
    }

    private SourceReader<String, Split> reader(AcknowledgementMode mode, Split split) throws Exception {
        SourceReader<String, Split> reader = source(mode).createReader(mock(SourceReaderContext.class));
        reader.addSplits(List.of(split));
        reader.start();
        ArgumentCaptor<Consumer> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(channel).basicConsume(eq("input"), eq(mode == AcknowledgementMode.AUTO_ACK), captor.capture());
        consumer = captor.getValue();
        return reader;
    }

    private Split emptySplit() {
        return new Split(0, Set.of(), false);
    }

    private void deliver(long tag, String id, boolean redelivery) throws IOException {
        consumer.handleDelivery("consumer", new Envelope(tag, redelivery, "", "input"),
                new AMQP.BasicProperties.Builder().correlationId(id).build(),
                "test-body".getBytes(StandardCharsets.UTF_8));
    }
}
