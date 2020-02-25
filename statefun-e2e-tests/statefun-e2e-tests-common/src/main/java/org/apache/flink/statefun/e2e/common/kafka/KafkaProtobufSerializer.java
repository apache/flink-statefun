/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.e2e.common.kafka;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A Kafka {@link Serializer} and {@link Deserializer} that uses Protobuf for serialization.
 *
 * @param <T> type of the Protobuf message.
 */
public final class KafkaProtobufSerializer<T extends Message>
    implements Serializer<T>, Deserializer<T> {

  private final Parser<T> parser;

  public KafkaProtobufSerializer(Parser<T> parser) {
    this.parser = Objects.requireNonNull(parser);
  }

  @Override
  public byte[] serialize(String s, T command) {
    return command.toByteArray();
  }

  @Override
  public T deserialize(String s, byte[] bytes) {
    try {
      return parser.parseFrom(bytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> map, boolean b) {}
}
