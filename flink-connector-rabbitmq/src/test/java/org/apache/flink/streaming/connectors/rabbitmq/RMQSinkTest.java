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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the {@link RMQSink}. */
public class RMQSinkTest {

    private static final String QUEUE_NAME = "queue";
    private static final String EXCHANGE = "exchange";
    private static final String ROUTING_KEY = "application.component.error";
    private static final String EXPIRATION = "10000";
    private static final String MESSAGE_STR = "msg";
    private static final byte[] MESSAGE = new byte[1];
    private static AMQP.BasicProperties props =
            new AMQP.BasicProperties.Builder()
                    .headers(Collections.singletonMap("Test", "My Value"))
                    .expiration(EXPIRATION)
                    .build();

    private RMQConnectionConfig rmqConnectionConfig;
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Channel channel;
    private WriterInitContext writerContext;
    private SerializationSchema<String> serializationSchema;
    private DummyPublishOptions publishOptions;
    private DummyReturnHandler returnListener;

    @Before
    public void before() throws Exception {
        serializationSchema = spy(new DummySerializationSchema());
        rmqConnectionConfig = mock(RMQConnectionConfig.class);
        connectionFactory = mock(ConnectionFactory.class);
        connection = mock(Connection.class);
        channel = mock(Channel.class);
        writerContext = mock(WriterInitContext.class);
        when(writerContext.asSerializationSchemaInitializationContext())
                .thenReturn(mock(SerializationSchema.InitializationContext.class));

        when(rmqConnectionConfig.getConnectionFactory()).thenReturn(connectionFactory);
        when(connectionFactory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
    }

    @Test
    public void openCallDeclaresQueueInStandardMode() throws Exception {
        createRMQSink();

        verify(channel).queueDeclare(QUEUE_NAME, true, false, false, null);
    }

    @Test
    public void openCallDontDeclaresQueueInWithOptionsMode() throws Exception {
        createRMQSinkWithOptions(false, false);

        verify(channel, never()).queueDeclare(null, true, false, false, null);
    }

    @Test
    public void testOverrideConnection() throws Exception {
        final Connection mockConnection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(mockConnection.createChannel()).thenReturn(channel);

        RMQSink<String> rmqSink =
                new RMQSink<String>(rmqConnectionConfig, QUEUE_NAME, serializationSchema) {
                    @Override
                    protected Connection setupConnection() throws Exception {
                        return mockConnection;
                    }
                };

        rmqSink.createWriter(writerContext);

        verify(mockConnection, times(1)).createChannel();
    }

    @Test
    public void throwExceptionIfChannelIsNull() throws Exception {
        when(connection.createChannel()).thenReturn(null);
        assertThrows(IOException.class, this::createRMQSink);
        verify(connection).close();
    }

    @Test
    public void testOpen() throws Exception {
        createRMQSink();
        verify(serializationSchema).open(writerContext.asSerializationSchemaInitializationContext());
    }

    private SinkWriter<String> createRMQSink() throws Exception {
        return new RMQSink<>(rmqConnectionConfig, QUEUE_NAME, serializationSchema)
                .createWriter(writerContext);
    }

    private SinkWriter<String> createRMQSinkWithOptions(boolean mandatory, boolean immediate)
            throws Exception {
        publishOptions = new DummyPublishOptions(mandatory, immediate);
        RMQSink<String> rmqSink =
                new RMQSink<>(rmqConnectionConfig, serializationSchema, publishOptions);
        return rmqSink.createWriter(writerContext);
    }

    private SinkWriter<String> createRMQSinkWithOptionsAndReturnHandler(
            boolean mandatory, boolean immediate) throws Exception {
        publishOptions = new DummyPublishOptions(mandatory, immediate);
        returnListener = new DummyReturnHandler();
        RMQSink<String> rmqSink =
                new RMQSink<>(
                        rmqConnectionConfig, serializationSchema, publishOptions, returnListener);
        return rmqSink.createWriter(writerContext);
    }

    @Test
    public void invokePublishBytesToQueue() throws Exception {
        SinkWriter<String> rmqSink = createRMQSink();

        rmqSink.write(MESSAGE_STR, null);
        verify(serializationSchema).serialize(MESSAGE_STR);
        verify(channel).basicPublish("", QUEUE_NAME, null, MESSAGE);
    }

    @Test
    public void exceptionDuringPublishingIsNotIgnored() throws Exception {
        SinkWriter<String> rmqSink = createRMQSink();

        doThrow(IOException.class).when(channel).basicPublish("", QUEUE_NAME, null, MESSAGE);
        assertThrows(IOException.class, () -> rmqSink.write("msg", null));
    }

    @Test
    public void exceptionDuringPublishingIsIgnoredIfLogFailuresOnly() throws Exception {
        RMQSink<String> sink = new RMQSink<>(rmqConnectionConfig, QUEUE_NAME, serializationSchema);
        sink.setLogFailuresOnly(true);
        SinkWriter<String> rmqSink = sink.createWriter(writerContext);

        doThrow(IOException.class).when(channel).basicPublish("", QUEUE_NAME, null, MESSAGE);
        rmqSink.write("msg", null);
    }

    @Test
    public void closeAllResources() throws Exception {
        SinkWriter<String> rmqSink = createRMQSink();

        rmqSink.close();

        verify(channel).close();
        verify(connection).close();
    }

    @Test
    public void invokePublishBytesToQueueWithOptions() throws Exception {
        SinkWriter<String> rmqSink = createRMQSinkWithOptions(false, false);

        rmqSink.write(MESSAGE_STR, null);
        verify(serializationSchema).serialize(MESSAGE_STR);
        verify(channel)
                .basicPublish(
                        EXCHANGE,
                        ROUTING_KEY,
                        false,
                        false,
                        publishOptions.computeProperties(""),
                        MESSAGE);
    }

    @Test
    public void invokePublishBytesToQueueWithOptionsMandatory() throws Exception {
        SinkWriter<String> rmqSink = createRMQSinkWithOptions(true, false);

        assertThrows(IllegalStateException.class, () -> rmqSink.write(MESSAGE_STR, null));
    }

    @Test
    public void invokePublishBytesToQueueWithOptionsImmediate() throws Exception {
        SinkWriter<String> rmqSink = createRMQSinkWithOptions(false, true);

        assertThrows(IllegalStateException.class, () -> rmqSink.write(MESSAGE_STR, null));
    }

    @Test
    public void invokePublishBytesToQueueWithOptionsMandatoryReturnHandler() throws Exception {
        SinkWriter<String> rmqSink = createRMQSinkWithOptionsAndReturnHandler(true, false);

        rmqSink.write(MESSAGE_STR, null);
        verify(serializationSchema).serialize(MESSAGE_STR);
        verify(channel)
                .basicPublish(
                        EXCHANGE,
                        ROUTING_KEY,
                        true,
                        false,
                        publishOptions.computeProperties(""),
                        MESSAGE);
    }

    @Test
    public void invokePublishBytesToQueueWithOptionsImmediateReturnHandler() throws Exception {
        SinkWriter<String> rmqSink = createRMQSinkWithOptionsAndReturnHandler(false, true);

        rmqSink.write(MESSAGE_STR, null);
        verify(serializationSchema).serialize(MESSAGE_STR);
        verify(channel)
                .basicPublish(
                        EXCHANGE,
                        ROUTING_KEY,
                        false,
                        true,
                        publishOptions.computeProperties(""),
                        MESSAGE);
    }

    @Test
    public void exceptionDuringWithOptionsPublishingIsNotIgnored() throws Exception {
        SinkWriter<String> rmqSink = createRMQSinkWithOptions(false, false);

        doThrow(IOException.class)
                .when(channel)
                .basicPublish(
                        EXCHANGE,
                        ROUTING_KEY,
                        false,
                        false,
                        publishOptions.computeProperties(""),
                        MESSAGE);
        assertThrows(IOException.class, () -> rmqSink.write("msg", null));
    }

    @Test
    public void exceptionDuringWithOptionsPublishingIsIgnoredIfLogFailuresOnly() throws Exception {
        publishOptions = new DummyPublishOptions(false, false);
        RMQSink<String> sink = new RMQSink<>(rmqConnectionConfig, serializationSchema, publishOptions);
        sink.setLogFailuresOnly(true);
        SinkWriter<String> rmqSink = sink.createWriter(writerContext);

        doThrow(IOException.class)
                .when(channel)
                .basicPublish(
                        EXCHANGE,
                        ROUTING_KEY,
                        false,
                        false,
                        publishOptions.computeProperties(""),
                        MESSAGE);
        rmqSink.write("msg", null);
    }

    private class DummyPublishOptions implements RMQSinkPublishOptions<String> {
        private static final long serialVersionUID = 1L;
        private boolean mandatory = false;
        private boolean immediate = false;

        public DummyPublishOptions(boolean mandatory, boolean immediate) {
            this.mandatory = mandatory;
            this.immediate = immediate;
        }

        @Override
        public String computeRoutingKey(String a) {
            return ROUTING_KEY;
        }

        @Override
        public BasicProperties computeProperties(String a) {
            return props;
        }

        @Override
        public String computeExchange(String a) {
            return EXCHANGE;
        }

        @Override
        public boolean computeMandatory(String a) {
            return mandatory;
        }

        @Override
        public boolean computeImmediate(String a) {
            return immediate;
        }
    }

    private class DummyReturnHandler implements SerializableReturnListener {

        private static final long serialVersionUID = 1L;

        @Override
        public void handleReturn(
                final int replyCode,
                final String replyText,
                final String exchange,
                final String routingKey,
                final BasicProperties properties,
                final byte[] body) {}
    }

    private class DummySerializationSchema implements SerializationSchema<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(String element) {
            return MESSAGE;
        }
    }
}
