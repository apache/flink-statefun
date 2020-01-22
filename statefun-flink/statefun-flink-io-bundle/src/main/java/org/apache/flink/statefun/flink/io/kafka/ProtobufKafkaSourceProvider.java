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
package org.apache.flink.statefun.flink.io.kafka;

import com.google.protobuf.Message;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.io.spi.JsonIngressSpec;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

final class ProtobufKafkaSourceProvider implements SourceProvider {

  private static final JsonPointer DESCRIPTOR_SET_POINTER =
      JsonPointer.compile("/ingress/spec/descriptorSet");
  private static final JsonPointer TOPICS_POINTER = JsonPointer.compile("/ingress/spec/topics");
  private static final JsonPointer MESSAGE_TYPE_POINTER =
      JsonPointer.compile("/ingress/spec/messageType");
  private static final JsonPointer PROPERTIES_POINTER =
      JsonPointer.compile("/ingress/spec/properties");
  private static final JsonPointer ADDRESS_POINTER = JsonPointer.compile("/ingress/spec/address");

  @Override
  public <T> SourceFunction<T> forSpec(IngressSpec<T> spec) {
    JsonNode json = asJsonIngressSpec(spec);
    Properties properties = kafkaClientProperties(json);
    List<String> topics = Selectors.textListAt(json, TOPICS_POINTER);
    KafkaDeserializationSchema<T> deserializationSchema = deserializationSchema(json);
    return new FlinkKafkaConsumer<>(topics, deserializationSchema, properties);
  }

  private <T> KafkaDeserializationSchema<T> deserializationSchema(JsonNode json) {
    String descriptorSetPath = Selectors.textAt(json, DESCRIPTOR_SET_POINTER);
    String messageType = Selectors.textAt(json, MESSAGE_TYPE_POINTER);
    // this cast is safe since we validate that the produced message type (T) is assignable to a
    // Message.
    // see asJsonIngressSpec()
    @SuppressWarnings("unchecked")
    KafkaIngressDeserializer<T> deserializer =
        (KafkaIngressDeserializer<T>)
            new ProtobufKafkaIngressDeserializer(descriptorSetPath, messageType);
    return new KafkaDeserializationSchemaDelegate<>(deserializer);
  }

  private static Properties kafkaClientProperties(JsonNode json) {
    Map<String, String> kvs = Selectors.propertiesAt(json, PROPERTIES_POINTER);
    Properties properties = new Properties();
    properties.put("bootstrap.servers", Selectors.textAt(json, ADDRESS_POINTER));
    kvs.forEach(properties::put);
    return properties;
  }

  private static JsonNode asJsonIngressSpec(IngressSpec<?> spec) {
    if (!(spec instanceof JsonIngressSpec)) {
      throw new IllegalArgumentException("Wrong type " + spec.type());
    }
    JsonIngressSpec<?> casted = (JsonIngressSpec<?>) spec;
    Class<?> producedType = casted.id().producedType();
    if (!Message.class.isAssignableFrom(producedType)) {
      throw new IllegalArgumentException(
          "ProtocolBuffer based ingress is able to produce types that derive from "
              + Message.class.getName()
              + " but "
              + producedType.getName()
              + " is provided.");
    }
    return casted.json();
  }
}
