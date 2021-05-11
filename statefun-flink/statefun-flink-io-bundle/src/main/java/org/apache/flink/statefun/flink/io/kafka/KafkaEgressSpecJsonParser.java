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

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.sdk.kafka.KafkaProducerSemantic;

final class KafkaEgressSpecJsonParser {

  private KafkaEgressSpecJsonParser() {}

  private static final JsonPointer PROPERTIES_POINTER =
      JsonPointer.compile("/egress/spec/properties");
  private static final JsonPointer ADDRESS_POINTER = JsonPointer.compile("/egress/spec/address");

  private static final JsonPointer DELIVERY_SEMANTICS_POINTER =
      JsonPointer.compile("/egress/spec/deliverySemantic");
  private static final JsonPointer DELIVERY_SEMANTICS_TYPE_POINTER =
      JsonPointer.compile("/egress/spec/deliverySemantic/type");

  /** @deprecated see {@link #DELIVERY_EXACTLY_ONCE_DURATION_TXN_TIMEOUT_POINTER}. */
  private static final JsonPointer DELIVERY_EXACTLY_ONCE_TXN_TIMEOUT_POINTER =
      JsonPointer.compile("/egress/spec/deliverySemantic/transactionTimeoutMillis");

  private static final JsonPointer DELIVERY_EXACTLY_ONCE_DURATION_TXN_TIMEOUT_POINTER =
      JsonPointer.compile("/egress/spec/deliverySemantic/transactionTimeout");

  static String kafkaAddress(JsonNode json) {
    return Selectors.textAt(json, ADDRESS_POINTER);
  }

  static Properties kafkaClientProperties(JsonNode json) {
    Map<String, String> kvs = Selectors.propertiesAt(json, PROPERTIES_POINTER);
    Properties properties = new Properties();
    kvs.forEach(properties::setProperty);
    return properties;
  }

  static Optional<KafkaProducerSemantic> optionalDeliverySemantic(JsonNode json) {
    if (json.at(DELIVERY_SEMANTICS_POINTER).isMissingNode()) {
      return Optional.empty();
    }

    String deliverySemanticType =
        Selectors.textAt(json, DELIVERY_SEMANTICS_TYPE_POINTER).toLowerCase(Locale.ENGLISH);
    switch (deliverySemanticType) {
      case "at-least-once":
        return Optional.of(KafkaProducerSemantic.AT_LEAST_ONCE);
      case "exactly-once":
        return Optional.of(KafkaProducerSemantic.EXACTLY_ONCE);
      case "none":
        return Optional.of(KafkaProducerSemantic.NONE);
      default:
        throw new IllegalArgumentException(
            "Invalid delivery semantic type: "
                + deliverySemanticType
                + "; valid types are [at-least-once, exactly-once, none]");
    }
  }

  static Duration exactlyOnceDeliveryTxnTimeout(JsonNode json) {
    // Prefer deprecated millis based timeout for backwards compatibility
    // then fallback to duration based configuration.
    OptionalLong transactionTimeoutMilli =
        Selectors.optionalLongAt(json, DELIVERY_EXACTLY_ONCE_TXN_TIMEOUT_POINTER);
    if (transactionTimeoutMilli.isPresent()) {
      return Duration.ofMillis(transactionTimeoutMilli.getAsLong());
    }

    return Selectors.durationAt(json, DELIVERY_EXACTLY_ONCE_DURATION_TXN_TIMEOUT_POINTER);
  }
}
