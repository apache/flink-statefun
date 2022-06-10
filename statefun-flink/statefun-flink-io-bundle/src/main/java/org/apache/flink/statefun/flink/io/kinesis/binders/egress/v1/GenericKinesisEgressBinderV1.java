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

package org.apache.flink.statefun.flink.io.kinesis.binders.egress.v1;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.egress.generated.KinesisEgressRecord;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * Version 1 {@link ComponentBinder} for binding a Kinesis egress which expects {@link
 * KinesisEgressRecord} as input, and writes the wrapped value bytes to Kinesis. Corresponding
 * {@link TypeName} is {@code io.statefun.kinesis.v1/egress}.
 *
 * <p>Below is an example YAML document of the {@link ComponentJsonObject} recognized by this
 * binder, with the expected types of each field:
 *
 * <pre>
 * kind: io.statefun.kinesis.v1/egress                                (typename)
 * spec:                                                              (object)
 *   id: com.foo.bar/my-egress                                        (typename)
 *   awsRegion:                                                       (object, optional)
 *     type: specific                                                 (string)
 *     id: us-west-2                                                  (string)
 *   awsCredentials:                                                  (object, optional)
 *     type: basic                                                    (string)
 *     accessKeyId: my_access_key_id                                  (string)
 *     secretAccessKey: my_secret_access_key                          (string)
 *   maxOutstandingRecords: 9999                                      (int, optional)
 *   clientConfigProperties:                                          (array, optional)
 *     - SocketTimeout: 9999                                          (string)
 *     - MaxConnections: 15                                           (string)
 *     - ...
 * </pre>
 *
 * <p>The {@code awsRegion} and {@code awsCredentials} options all have multiple options to choose
 * from. Please see {@link GenericKinesisEgressSpec} for further details.
 */
final class GenericKinesisEgressBinderV1 implements ComponentBinder {

  private static final ObjectMapper SPEC_OBJ_MAPPER = StateFunObjectMapper.create();

  static final GenericKinesisEgressBinderV1 INSTANCE = new GenericKinesisEgressBinderV1();

  static final TypeName KIND_TYPE = TypeName.parseFrom("io.statefun.kinesis.v1/egress");

  private GenericKinesisEgressBinderV1() {}

  @Override
  public void bind(
      ComponentJsonObject component, StatefulFunctionModule.Binder remoteModuleBinder) {
    validateComponent(component);

    final JsonNode specJsonNode = component.specJsonNode();
    final GenericKinesisEgressSpec spec = parseSpec(specJsonNode);
    remoteModuleBinder.bindEgress(spec.toUniversalKinesisEgressSpec());
  }

  private static void validateComponent(ComponentJsonObject componentJsonObject) {
    final TypeName targetBinderType = componentJsonObject.binderTypename();
    if (!targetBinderType.equals(KIND_TYPE)) {
      throw new IllegalStateException(
          "Received unexpected ModuleComponent to bind: " + componentJsonObject);
    }
  }

  private static GenericKinesisEgressSpec parseSpec(JsonNode specJsonNode) {
    try {
      return SPEC_OBJ_MAPPER.treeToValue(specJsonNode, GenericKinesisEgressSpec.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error parsing a GenericKinesisEgressSpec.", e);
    }
  }
}
