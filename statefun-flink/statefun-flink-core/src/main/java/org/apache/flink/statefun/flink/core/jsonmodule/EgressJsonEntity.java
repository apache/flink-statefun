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

package org.apache.flink.statefun.flink.core.jsonmodule;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.NamespaceNamePair;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.io.spi.JsonEgressSpec;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule.Binder;

final class EgressJsonEntity implements JsonEntity {

  private static final JsonPointer EGRESS_SPECS_POINTER = JsonPointer.compile("/egresses");

  private static final class MetaPointers {
    private static final JsonPointer ID = JsonPointer.compile("/egress/meta/id");
    private static final JsonPointer TYPE = JsonPointer.compile("/egress/meta/type");
  }

  @Override
  public void bind(Binder binder, JsonNode moduleSpecRootNode, FormatVersion formatVersion) {
    final Iterable<? extends JsonNode> egressNodes =
        Selectors.listAt(moduleSpecRootNode, EGRESS_SPECS_POINTER);

    egressNodes.forEach(
        egressNode -> {
          binder.bindEgress(
              new JsonEgressSpec<>(egressType(egressNode), egressId(egressNode), egressNode));
        });
  }

  private static EgressType egressType(JsonNode spec) {
    String typeString = Selectors.textAt(spec, MetaPointers.TYPE);
    NamespaceNamePair nn = NamespaceNamePair.from(typeString);
    return new EgressType(nn.namespace(), nn.name());
  }

  private static EgressIdentifier<TypedValue> egressId(JsonNode spec) {
    String egressId = Selectors.textAt(spec, MetaPointers.ID);
    NamespaceNamePair nn = NamespaceNamePair.from(egressId);
    return new EgressIdentifier<>(nn.namespace(), nn.name(), TypedValue.class);
  }
}
