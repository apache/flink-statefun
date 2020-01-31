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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.io.spi.JsonIngressSpec;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressAutoResetPosition;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressBuilderApiExtension;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressStartupPosition;
import org.apache.flink.statefun.sdk.kafka.KafkaTopicPartition;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

final class ProtobufKafkaSourceProvider implements SourceProvider {

  private static final JsonPointer DESCRIPTOR_SET_POINTER =
      JsonPointer.compile("/ingress/spec/descriptorSet");
  private static final JsonPointer TOPICS_POINTER = JsonPointer.compile("/ingress/spec/topics");
  private static final JsonPointer MESSAGE_TYPE_POINTER =
      JsonPointer.compile("/ingress/spec/messageType");
  private static final JsonPointer PROPERTIES_POINTER =
      JsonPointer.compile("/ingress/spec/properties");
  private static final JsonPointer ADDRESS_POINTER = JsonPointer.compile("/ingress/spec/address");
  private static final JsonPointer GROUP_ID_POINTER =
      JsonPointer.compile("/ingress/spec/consumerGroupId");
  private static final JsonPointer AUTO_RESET_POS_POINTER =
      JsonPointer.compile("/ingress/spec/autoOffsetResetPosition");

  private static final JsonPointer STARTUP_POS_POINTER =
      JsonPointer.compile("/ingress/spec/startupPosition");
  private static final JsonPointer STARTUP_POS_TYPE_POINTER =
      JsonPointer.compile("/ingress/spec/startupPosition/type");
  private static final JsonPointer STARTUP_SPECIFIC_OFFSETS_POINTER =
      JsonPointer.compile("/ingress/spec/startupPosition/offsets");
  private static final JsonPointer STARTUP_DATE_POINTER =
      JsonPointer.compile("/ingress/spec/startupPosition/date");

  private static final String STARTUP_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS Z";

  private final KafkaSourceProvider delegateProvider = new KafkaSourceProvider();

  @Override
  public <T> SourceFunction<T> forSpec(IngressSpec<T> spec) {
    KafkaIngressSpec<T> kafkaIngressSpec = asKafkaIngressSpec(spec);
    return delegateProvider.forSpec(kafkaIngressSpec);
  }

  private static <T> KafkaIngressSpec<T> asKafkaIngressSpec(IngressSpec<T> spec) {
    if (!(spec instanceof JsonIngressSpec)) {
      throw new IllegalArgumentException("Wrong type " + spec.type());
    }
    JsonIngressSpec<T> casted = (JsonIngressSpec<T>) spec;

    IngressIdentifier<T> id = casted.id();
    Class<?> producedType = casted.id().producedType();
    if (!Message.class.isAssignableFrom(producedType)) {
      throw new IllegalArgumentException(
          "ProtocolBuffer based ingress is only able to produce types that derive from "
              + Message.class.getName()
              + " but "
              + producedType.getName()
              + " is provided.");
    }

    JsonNode json = casted.json();

    KafkaIngressBuilder<T> kafkaIngressBuilder = KafkaIngressBuilder.forIdentifier(id);
    kafkaIngressBuilder
        .withKafkaAddress(kafkaAddress(json))
        .withProperties(kafkaClientProperties(json))
        .addTopics(topics(json));

    optionalConsumerGroupId(json).ifPresent(kafkaIngressBuilder::withConsumerGroupId);
    optionalAutoOffsetResetPosition(json).ifPresent(kafkaIngressBuilder::withAutoResetPosition);
    optionalStartupPosition(json).ifPresent(kafkaIngressBuilder::withStartupPosition);

    KafkaIngressBuilderApiExtension.withDeserializer(kafkaIngressBuilder, deserializer(json));

    return kafkaIngressBuilder.build();
  }

  private static List<String> topics(JsonNode json) {
    return Selectors.textListAt(json, TOPICS_POINTER);
  }

  private static Properties kafkaClientProperties(JsonNode json) {
    Map<String, String> kvs = Selectors.propertiesAt(json, PROPERTIES_POINTER);
    Properties properties = new Properties();
    kvs.forEach(properties::put);
    return properties;
  }

  private static String kafkaAddress(JsonNode json) {
    return Selectors.textAt(json, ADDRESS_POINTER);
  }

  @SuppressWarnings("unchecked")
  private static <T> KafkaIngressDeserializer<T> deserializer(JsonNode json) {
    String descriptorSetPath = Selectors.textAt(json, DESCRIPTOR_SET_POINTER);
    String messageType = Selectors.textAt(json, MESSAGE_TYPE_POINTER);
    // this cast is safe since we validate that the produced message type (T) is assignable to a
    // Message.
    // see asJsonIngressSpec()
    return (KafkaIngressDeserializer<T>)
        new ProtobufKafkaIngressDeserializer(descriptorSetPath, messageType);
  }

  private static Optional<String> optionalConsumerGroupId(JsonNode json) {
    return Selectors.optionalTextAt(json, GROUP_ID_POINTER);
  }

  private static Optional<KafkaIngressAutoResetPosition> optionalAutoOffsetResetPosition(
      JsonNode json) {
    Optional<String> conf = Selectors.optionalTextAt(json, AUTO_RESET_POS_POINTER);
    if (!conf.isPresent()) {
      return Optional.empty();
    }

    String autoOffsetResetConfig = conf.get().toUpperCase(Locale.ENGLISH);

    try {
      return Optional.of(KafkaIngressAutoResetPosition.valueOf(autoOffsetResetConfig));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid autoOffsetResetPosition: "
              + autoOffsetResetConfig
              + "; valid values are "
              + Arrays.toString(KafkaIngressAutoResetPosition.values()),
          e);
    }
  }

  private static Optional<KafkaIngressStartupPosition> optionalStartupPosition(JsonNode json) {
    if (json.at(STARTUP_POS_POINTER).isMissingNode()) {
      return Optional.empty();
    }

    String startupType =
        Selectors.textAt(json, STARTUP_POS_TYPE_POINTER).toLowerCase(Locale.ENGLISH);
    switch (startupType) {
      case "group-offsets":
        return Optional.of(KafkaIngressStartupPosition.fromGroupOffsets());
      case "earliest":
        return Optional.of(KafkaIngressStartupPosition.fromEarliest());
      case "latest":
        return Optional.of(KafkaIngressStartupPosition.fromLatest());
      case "specific-offsets":
        return Optional.of(
            KafkaIngressStartupPosition.fromSpecificOffsets(specificOffsetsStartupMap(json)));
      case "date":
        return Optional.of(KafkaIngressStartupPosition.fromDate(startupDate(json)));
      default:
        throw new IllegalArgumentException(
            "Invalid startup position type: "
                + startupType
                + "; valid values are [group-offsets, earliest, latest, specific-offsets, date]");
    }
  }

  private static Map<KafkaTopicPartition, Long> specificOffsetsStartupMap(JsonNode json) {
    Map<String, Long> kvs = Selectors.longPropertiesAt(json, STARTUP_SPECIFIC_OFFSETS_POINTER);
    Map<KafkaTopicPartition, Long> offsets = new HashMap<>(kvs.size());
    kvs.forEach(
        (partition, offset) ->
            offsets.put(KafkaTopicPartition.fromString(partition), validateOffsetLong(offset)));
    return offsets;
  }

  private static Date startupDate(JsonNode json) {
    String dateStr = Selectors.textAt(json, STARTUP_DATE_POINTER);
    SimpleDateFormat dateFormat = new SimpleDateFormat(STARTUP_DATE_FORMAT);
    try {
      return dateFormat.parse(dateStr);
    } catch (ParseException e) {
      throw new IllegalArgumentException(
          "Unable to parse date string for startup position: "
              + dateStr
              + "; the date should conform to the pattern "
              + STARTUP_DATE_FORMAT,
          e);
    }
  }

  private static Long validateOffsetLong(Long offset) {
    if (offset < 0) {
      throw new IllegalArgumentException(
          "Invalid offset value: "
              + offset
              + "; must be a numeric integer with value between 0 and "
              + Long.MAX_VALUE);
    }

    return offset;
  }
}
