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

package org.apache.flink.statefun.flink.io.kafka.binders;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.io.common.json.EgressIdentifierJsonDeserializer;
import org.apache.flink.statefun.flink.io.common.json.PropertiesJsonDeserializer;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaProducerSemantic;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.util.TimeUtils;

@JsonDeserialize(builder = GenericKafkaEgressSpec.Builder.class)
final class GenericKafkaEgressSpec {

  private final EgressIdentifier<TypedValue> id;
  private final Optional<String> address;
  private final KafkaProducerSemantic producerSemantic;
  private final Properties properties;

  private GenericKafkaEgressSpec(
      EgressIdentifier<TypedValue> id,
      Optional<String> address,
      KafkaProducerSemantic producerSemantic,
      Properties properties) {
    this.id = Objects.requireNonNull(id);
    this.address = Objects.requireNonNull(address);
    this.producerSemantic = Objects.requireNonNull(producerSemantic);
    this.properties = Objects.requireNonNull(properties);
  }

  public KafkaEgressSpec<TypedValue> toUniversalKafkaEgressSpec() {
    final KafkaEgressBuilder<TypedValue> builder = KafkaEgressBuilder.forIdentifier(id);
    address.ifPresent(builder::withKafkaAddress);
    builder.withProducerSemantic(producerSemantic);
    builder.withProperties(properties);
    builder.withSerializer(GenericKafkaEgressSerializer.class);
    return builder.build();
  }

  @JsonPOJOBuilder
  public static class Builder {

    private final EgressIdentifier<TypedValue> id;

    private Optional<String> kafkaAddress = Optional.empty();
    private KafkaProducerSemantic producerSemantic = KafkaProducerSemantic.atLeastOnce();
    private Properties properties = new Properties();

    @JsonCreator
    private Builder(
        @JsonProperty("id") @JsonDeserialize(using = EgressIdentifierJsonDeserializer.class)
            EgressIdentifier<TypedValue> id) {
      this.id = Objects.requireNonNull(id);
    }

    @JsonProperty("address")
    public Builder withKafkaAddress(String address) {
      Objects.requireNonNull(address);
      this.kafkaAddress = Optional.of(address);
      return this;
    }

    @JsonProperty("deliverySemantic")
    @JsonDeserialize(using = ProducerSemanticJsonDeserializer.class)
    public Builder withDeliverySemantic(KafkaProducerSemantic producerSemantic) {
      this.producerSemantic = Objects.requireNonNull(producerSemantic);
      return this;
    }

    @JsonProperty("properties")
    @JsonDeserialize(using = PropertiesJsonDeserializer.class)
    public Builder withProperties(Properties properties) {
      this.properties = Objects.requireNonNull(properties);
      return this;
    }

    public GenericKafkaEgressSpec build() {
      return new GenericKafkaEgressSpec(id, kafkaAddress, producerSemantic, properties);
    }
  }

  private static class ProducerSemanticJsonDeserializer
      extends JsonDeserializer<KafkaProducerSemantic> {
    @Override
    public KafkaProducerSemantic deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      final ObjectNode producerSemanticNode = jsonParser.readValueAs(ObjectNode.class);
      final String semanticTypeString = producerSemanticNode.get("type").asText();
      switch (semanticTypeString) {
        case "at-least-once":
          return KafkaProducerSemantic.atLeastOnce();
        case "exactly-once":
          return KafkaProducerSemantic.exactlyOnce(parseTransactionTimeout(producerSemanticNode));
        case "none":
          return KafkaProducerSemantic.none();
        default:
          throw new IllegalArgumentException(
              "Invalid delivery semantic type: "
                  + semanticTypeString
                  + "; valid types are [at-least-once, exactly-once, none]");
      }
    }
  }

  private static Duration parseTransactionTimeout(ObjectNode producerSemanticNode) {
    // Prefer deprecated millis based timeout for backwards compatibility
    // then fallback to duration based configuration.
    final JsonNode deprecatedTransactionTimeoutMillisNode =
        producerSemanticNode.get("transactionTimeoutMillis");
    if (deprecatedTransactionTimeoutMillisNode != null) {
      return Duration.ofMillis(deprecatedTransactionTimeoutMillisNode.asLong());
    }
    return TimeUtils.parseDuration(producerSemanticNode.get("transactionTimeout").asText());
  }
}
