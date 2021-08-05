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

package org.apache.flink.statefun.extensions;

import java.util.Objects;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * A {@link ComponentJsonObject} consists one or more application entities (i.e. function providers,
 * ingresses, routers, or egresses) that should be bound to a remote {@link StatefulFunctionModule}.
 *
 * <p>Each component is represented in remote module YAML specification files as a single YAML
 * document of the following format:
 *
 * <pre>
 * kind: com.foo.bar.v5/some.component         (typename)
 * spec:
 *   ...                                       (specification document of the component)
 * </pre>
 *
 * <p>The {@code kind} is a {@link TypeName} that should be universally unique within the
 * application. It identifies which {@link ComponentBinder} recognizes this component and knows how
 * to parse it to resolve application entities to be bound to the module.
 *
 * @see ComponentBinder
 */
@PublicEvolving
public final class ComponentJsonObject {

  public static final String BINDER_KIND_FIELD = "kind";
  public static final String SPEC_FIELD = "spec";

  private final ObjectNode rawObjectNode;

  private final TypeName binderTypename;
  private final JsonNode specJsonNode;

  public ComponentJsonObject(JsonNode jsonNode) {
    Objects.requireNonNull(jsonNode);

    checkIsObject(jsonNode);
    this.rawObjectNode = (ObjectNode) jsonNode;

    this.binderTypename = parseBinderTypename(rawObjectNode);
    this.specJsonNode = extractSpecJsonNode(rawObjectNode);
  }

  /**
   * Returns the complete component JSON object.
   *
   * @return the complete component JSON object.
   */
  public ObjectNode get() {
    return rawObjectNode;
  }

  /**
   * Returns the {@link TypeName} of the binder for this component.
   *
   * @return the {@link TypeName} of the binder for this component.
   */
  public TypeName binderTypename() {
    return binderTypename;
  }

  /**
   * Returns the specification JSON node for this component.
   *
   * @return the specification JSON node for this component.
   */
  public JsonNode specJsonNode() {
    return specJsonNode;
  }

  @Override
  public String toString() {
    return rawObjectNode.toString();
  }

  private static void checkIsObject(JsonNode jsonNode) {
    if (!jsonNode.isObject()) {
      throwExceptionWithFormatHint();
    }
  }

  private static TypeName parseBinderTypename(ObjectNode componentObject) {
    final JsonNode binderKindObject = componentObject.get(BINDER_KIND_FIELD);
    if (binderKindObject == null) {
      throwExceptionWithFormatHint();
    }

    try {
      return TypeName.parseFrom(binderKindObject.asText());
    } catch (Exception e) {
      throw new ComponentJsonFormatException("Invalid binder kind format.", e);
    }
  }

  private static JsonNode extractSpecJsonNode(ObjectNode componentObject) {
    final JsonNode specJsonNode = componentObject.get(SPEC_FIELD);
    if (specJsonNode == null) {
      throwExceptionWithFormatHint();
    }
    return specJsonNode;
  }

  private static void throwExceptionWithFormatHint() {
    throw new ComponentJsonFormatException(
        "Invalid ComponentJsonObject; components should be a JSON object with the required fields [kind] and [spec].");
  }
}
