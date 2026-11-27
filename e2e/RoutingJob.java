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
import java.nio.file.Files;
import java.nio.file.Path;

import com.rabbitmq.client.AMQP;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource.AcknowledgementMode;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/** RabbitMQ-to-RabbitMQ round trip without checkpointing, with exchange routing and returns. */
public final class RoutingJob {
    public static void main(String[] args) throws Exception {
        RMQConnectionConfig config = new RMQConnectionConfig.Builder()
                .setUri("amqp://guest:guest@127.0.0.1:" + args[0]).build();
        String marker = args[1];
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        new RMQSource<>(config, "route-input", AcknowledgementMode.AUTO_ACK, new SimpleStringSchema())
                .addTo(env, WatermarkStrategy.noWatermarks(), "rabbitmq")
                .sinkTo(new RMQSink<>(config, new SimpleStringSchema(), new RouteOptions(),
                        (code, text, exchange, key, props, body) -> {
                            try {
                                Files.writeString(Path.of(marker), code + ":" + key);
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        }));
        env.execute("rabbitmq-routing");
    }

    private static final class RouteOptions implements RMQSinkPublishOptions<String> {
        @Override
        public String computeRoutingKey(String value) {
            return value;
        }

        @Override
        public AMQP.BasicProperties computeProperties(String value) {
            return new AMQP.BasicProperties.Builder().correlationId("routed-" + value)
                    .contentType("text/plain").build();
        }

        @Override
        public String computeExchange(String value) {
            return "route-exchange";
        }

        @Override
        public boolean computeMandatory(String value) {
            return true;
        }
    }
}
