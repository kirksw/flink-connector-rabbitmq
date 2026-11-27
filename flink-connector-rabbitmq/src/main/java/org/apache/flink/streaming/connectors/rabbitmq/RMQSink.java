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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Preconditions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A Sink for publishing data into RabbitMQ.
 *
 * @param <IN>
 */
@PublicEvolving
public class RMQSink<IN> extends RMQSinkAdapter<IN> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RMQSink.class);

    @Nullable protected final String queueName;

    private final RMQConnectionConfig rmqConnectionConfig;
    private final SerializationSchema<IN> schema;
    private boolean logFailuresOnly = false;

    @Nullable private final RMQSinkPublishOptions<IN> publishOptions;

    @Nullable private final SerializableReturnListener returnListener;

    /**
     * @param rmqConnectionConfig The RabbitMQ connection configuration {@link RMQConnectionConfig}.
     * @param queueName The queue to publish messages to.
     * @param schema A {@link SerializationSchema} for turning the Java objects received into bytes
     * @param publishOptions A {@link RMQSinkPublishOptions} for providing message's routing key
     *     and/or properties
     * @param returnListener A SerializableReturnListener implementation object to handle returned
     *     message event
     */
    private RMQSink(
            RMQConnectionConfig rmqConnectionConfig,
            @Nullable String queueName,
            SerializationSchema<IN> schema,
            @Nullable RMQSinkPublishOptions<IN> publishOptions,
            @Nullable SerializableReturnListener returnListener) {
        this.rmqConnectionConfig = rmqConnectionConfig;
        this.queueName = queueName;
        this.schema = schema;
        this.publishOptions = publishOptions;
        this.returnListener = returnListener;
    }

    /**
     * @param rmqConnectionConfig The RabbitMQ connection configuration {@link RMQConnectionConfig}.
     * @param queueName The queue to publish messages to.
     * @param schema A {@link SerializationSchema} for turning the Java objects received into bytes
     */
    @PublicEvolving
    public RMQSink(
            RMQConnectionConfig rmqConnectionConfig,
            String queueName,
            SerializationSchema<IN> schema) {
        this(rmqConnectionConfig, queueName, schema, null, null);
    }

    /**
     * @param rmqConnectionConfig The RabbitMQ connection configuration {@link RMQConnectionConfig}.
     * @param schema A {@link SerializationSchema} for turning the Java objects received into bytes
     * @param publishOptions A {@link RMQSinkPublishOptions} for providing message's routing key
     *     and/or properties In this case the computeMandatoy or computeImmediate MUST return false
     *     otherwise an IllegalStateException is raised during runtime.
     */
    @PublicEvolving
    public RMQSink(
            RMQConnectionConfig rmqConnectionConfig,
            SerializationSchema<IN> schema,
            RMQSinkPublishOptions<IN> publishOptions) {
        this(rmqConnectionConfig, null, schema, publishOptions, null);
    }

    /**
     * @param rmqConnectionConfig The RabbitMQ connection configuration {@link RMQConnectionConfig}.
     * @param schema A {@link SerializationSchema} for turning the Java objects received into bytes
     * @param publishOptions A {@link RMQSinkPublishOptions} for providing message's routing key
     *     and/or properties
     * @param returnListener A SerializableReturnListener implementation object to handle returned
     *     message event
     */
    @PublicEvolving
    public RMQSink(
            RMQConnectionConfig rmqConnectionConfig,
            SerializationSchema<IN> schema,
            RMQSinkPublishOptions<IN> publishOptions,
            SerializableReturnListener returnListener) {
        this(rmqConnectionConfig, null, schema, publishOptions, returnListener);
    }

    /**
     * Sets up the queue. The default implementation just declares the queue. The user may override
     * this method to have a custom setup for the queue (i.e. binding the queue to an exchange or
     * defining custom queue parameters)
     */
    protected void setupQueue(Channel channel) throws IOException {
        if (queueName != null) {
            Util.declareQueueDefaults(channel, queueName);
        }
    }

    /**
     * Defines whether the producer should fail on errors, or only log them. If this is set to true,
     * then exceptions will be only logged, if set to false, exceptions will be eventually thrown
     * and cause the streaming program to fail (and enter recovery).
     *
     * @param logFailuresOnly The flag to indicate logging-only on exceptions.
     */
    public void setLogFailuresOnly(boolean logFailuresOnly) {
        this.logFailuresOnly = logFailuresOnly;
    }

    /**
     * Initializes the connection to RMQ with a default connection factory. The user may override
     * this method to setup and configure their own {@link ConnectionFactory}.
     */
    protected ConnectionFactory setupConnectionFactory() throws Exception {
        return rmqConnectionConfig.getConnectionFactory();
    }

    /**
     * Initializes the connection to RMQ using the default connection factory from {@link
     * #setupConnectionFactory()}. The user may override this method to setup and configure their
     * own {@link Connection}.
     */
    protected Connection setupConnection() throws Exception {
        return setupConnectionFactory().newConnection();
    }

    @Override
    SinkWriter<IN> openWriter(SerializationSchema.InitializationContext context)
            throws IOException {
        Writer writer = new Writer();
        try {
            schema.open(context);
            writer.connection = setupConnection();
            writer.channel = writer.connection.createChannel();
            if (writer.channel == null) {
                throw new IOException("None of RabbitMQ channels are available");
            }
            setupQueue(writer.channel);
            if (returnListener != null) {
                writer.channel.addReturnListener(returnListener);
            }
            return writer;
        } catch (Exception e) {
            try {
                writer.close();
            } catch (Exception closeError) {
                e.addSuppressed(closeError);
            }
            throw new IOException("Could not initialize RabbitMQ sink", e);
        }
    }

    private final class Writer implements SinkWriter<IN> {
        private Connection connection;
        private Channel channel;

        @Override
        public void write(IN value, Context context) throws IOException {
            try {
                byte[] msg = schema.serialize(value);

                if (publishOptions == null) {
                    channel.basicPublish("", queueName, null, msg);
                } else {
                    boolean mandatory = publishOptions.computeMandatory(value);
                    boolean immediate = publishOptions.computeImmediate(value);

                    Preconditions.checkState(
                            !(returnListener == null && (mandatory || immediate)),
                            "Setting mandatory and/or immediate flags to true requires a ReturnListener.");

                    String rk = publishOptions.computeRoutingKey(value);
                    String exchange = publishOptions.computeExchange(value);

                    channel.basicPublish(
                            exchange,
                            rk,
                            mandatory,
                            immediate,
                            publishOptions.computeProperties(value),
                            msg);
                }
            } catch (IOException e) {
                if (logFailuresOnly) {
                    LOG.error(
                            "Cannot send RMQ message {} at {}",
                            queueName,
                            rmqConnectionConfig.getHost(),
                            e);
                } else {
                    throw new IOException(
                            "Cannot send RMQ message "
                                    + queueName
                                    + " at "
                                    + rmqConnectionConfig.getHost(),
                            e);
                }
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            // Publish calls are synchronous; this sink does not enable publisher confirms.
        }

        @Override
        public void close() throws Exception {
            try (Connection openConnection = connection; Channel openChannel = channel) {
                // Close channel before connection, preserving errors from both.
            }
        }
    }
}
