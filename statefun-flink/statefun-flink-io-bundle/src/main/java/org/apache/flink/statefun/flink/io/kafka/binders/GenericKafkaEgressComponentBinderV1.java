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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.extensions.ExtensionResolver;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * Version 1 {@link ComponentBinder} for binding a Kafka egress which expects {@link
 * KafkaProducerRecord} as input, and writes the wrapped value bytes to Kafka. Corresponding {@link
 * TypeName} is {@code io.statefun.kafka.v1/egress}.
 *
 * <p>Below is an example YAML document of the {@link ComponentJsonObject} recognized by this
 * binder, with the expected types of each field:
 *
 * <pre>
 * kind: io.statefun.kafka.v1/egress                                  (typename)
 * spec:                                                              (object)
 *   id: com.foo.bar/my-ingress                                       (typename)
 *   address: kafka-broker:9092                                       (string, optional)
 *   deliverySemantic:                                                (object, optional)
 *     type: exactly-once                                             (string)
 *     transactionTimeout: 15min                                      (duration)
 *   properties:                                                      (array)
 *     - foo.config: bar                                              (string)
 * </pre>
 *
 * <p>The {@code deliverySemantic} can be one of the following options: {@code exactly-once}, {@code
 * at-least-once}, or {@code none}.
 *
 * <p>Please see {@link GenericKafkaEgressSpec} for further details.
 */
final class GenericKafkaEgressComponentBinderV1 implements ComponentBinder {
  private static final ObjectMapper SPEC_OBJ_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  static final GenericKafkaEgressComponentBinderV1 INSTANCE =
      new GenericKafkaEgressComponentBinderV1();

  static final TypeName ALTERNATIVE_KIND_TYPE = TypeName.parseFrom("io.statefun.kafka/egress");
  static final TypeName KIND_TYPE = TypeName.parseFrom("io.statefun.kafka.v1/egress");

  private GenericKafkaEgressComponentBinderV1() {}

  @Override
  public void bind(
      ComponentJsonObject component,
      StatefulFunctionModule.Binder binder,
      ExtensionResolver extensionResolver) {
    validateComponent(component);

    final JsonNode specJsonNode = component.specJsonNode();
    final GenericKafkaEgressSpec spec = parseSpec(specJsonNode);
    binder.bindEgress(spec.toUniversalKafkaEgressSpec());
  }

  private static void validateComponent(ComponentJsonObject componentJsonObject) {
    final TypeName targetBinderType = componentJsonObject.binderTypename();
    if (!targetBinderType.equals(KIND_TYPE) && !targetBinderType.equals(ALTERNATIVE_KIND_TYPE)) {
      throw new IllegalStateException(
          "Received unexpected ModuleComponent to bind: " + componentJsonObject);
    }
  }

  private static GenericKafkaEgressSpec parseSpec(JsonNode specJsonNode) {
    try {
      return SPEC_OBJ_MAPPER.treeToValue(specJsonNode, GenericKafkaEgressSpec.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error parsing a GenericKafkaEgressSpec.", e);
    }
  }
}
