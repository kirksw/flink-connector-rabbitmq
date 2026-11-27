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

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** A standalone cluster smoke job with no RabbitMQ connector dependency. */
public final class ControlJob {
    public static void main(String[] args) throws Exception {
        String marker = args[0];
        try (var records = StreamExecutionEnvironment.getExecutionEnvironment()
                .fromElements("flink-ok")
                .map(value -> Files.writeString(Path.of(marker), value).toString())
                .executeAndCollect()) {
            if (!marker.equals(records.next())) {
                throw new IllegalStateException("The control job did not write its marker");
            }
        }
    }
}
