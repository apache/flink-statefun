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

package org.apache.flink.statefun.flink.common.json;

import java.io.IOException;
import java.util.Objects;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.statefun.flink.common.extensions.ComponentBinder;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * A {@link ModuleComponent} consists one or more application entities (i.e. function providers,
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
public final class ModuleComponent {

  private final TypeName binderTypename;
  private final JsonNode specJsonNode;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public ModuleComponent(
      @JsonProperty("kind") @JsonDeserialize(using = TypeNameDeserializer.class)
          TypeName binderTypename,
      @JsonProperty("spec") JsonNode specJsonNode) {
    this.binderTypename = Objects.requireNonNull(binderTypename);
    this.specJsonNode = Objects.requireNonNull(specJsonNode);
  }

  public TypeName binderTypename() {
    return binderTypename;
  }

  public JsonNode specJsonNode() {
    return specJsonNode;
  }

  private static class TypeNameDeserializer extends JsonDeserializer<TypeName> {
    @Override
    public TypeName deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      return TypeName.parseFrom(jsonParser.getText());
    }
  }

  @Override
  public String toString() {
    return "ModuleComponent{" + "kind=" + binderTypename + ", spec=" + specJsonNode + '}';
  }
}
