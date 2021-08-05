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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.extensions.ExtensionResolver;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

@Deprecated
public final class LegacyRemoteModuleV30 implements StatefulFunctionModule {

  private final JsonNode moduleSpecNode;

  // =====================================================================
  //  Json pointers for backwards compatibility
  // =====================================================================

  private static final JsonPointer ENDPOINTS = JsonPointer.compile("/endpoints");
  private static final JsonPointer INGRESSES = JsonPointer.compile("/ingresses");
  private static final JsonPointer EGRESSES = JsonPointer.compile("/egresses");

  private static final JsonPointer ENDPOINT_KIND = JsonPointer.compile("/endpoint/meta/kind");
  private static final JsonPointer ENDPOINT_SPEC = JsonPointer.compile("/endpoint/spec");
  private static final JsonPointer INGRESS_KIND = JsonPointer.compile("/ingress/meta/type");
  private static final JsonPointer INGRESS_ID = JsonPointer.compile("/ingress/meta/id");
  private static final JsonPointer INGRESS_SPEC = JsonPointer.compile("/ingress/spec");
  private static final JsonPointer EGRESS_KIND = JsonPointer.compile("/egress/meta/type");
  private static final JsonPointer EGRESS_ID = JsonPointer.compile("/egress/meta/id");
  private static final JsonPointer EGRESS_SPEC = JsonPointer.compile("/egress/spec");

  LegacyRemoteModuleV30(JsonNode moduleSpecNode) {
    this.moduleSpecNode = Objects.requireNonNull(moduleSpecNode);
  }

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder moduleBinder) {
    components(moduleSpecNode).forEach(component -> bindComponent(component, moduleBinder));
  }

  private static Iterable<ComponentJsonObject> components(JsonNode moduleRootNode) {
    final List<ComponentJsonObject> components = new ArrayList<>();
    components.addAll(endpointComponents(moduleRootNode));
    components.addAll(ingressComponents(moduleRootNode));
    components.addAll(egressComponents(moduleRootNode));
    return components;
  }

  private static List<ComponentJsonObject> endpointComponents(JsonNode moduleRootNode) {
    final Iterable<? extends JsonNode> endpointComponentNodes =
        Selectors.listAt(moduleRootNode, ENDPOINTS);
    return StreamSupport.stream(endpointComponentNodes.spliterator(), false)
        .map(LegacyRemoteModuleV30::parseEndpointComponentNode)
        .collect(Collectors.toList());
  }

  private static List<ComponentJsonObject> ingressComponents(JsonNode moduleRootNode) {
    final Iterable<? extends JsonNode> ingressComponentNodes =
        Selectors.listAt(moduleRootNode, INGRESSES);
    return StreamSupport.stream(ingressComponentNodes.spliterator(), false)
        .map(LegacyRemoteModuleV30::parseIngressComponentNode)
        .collect(Collectors.toList());
  }

  private static List<ComponentJsonObject> egressComponents(JsonNode moduleRootNode) {
    final Iterable<? extends JsonNode> egressComponentNodes =
        Selectors.listAt(moduleRootNode, EGRESSES);
    return StreamSupport.stream(egressComponentNodes.spliterator(), false)
        .map(LegacyRemoteModuleV30::parseEgressComponentNode)
        .collect(Collectors.toList());
  }

  private static ComponentJsonObject parseEndpointComponentNode(JsonNode node) {
    final String endpointKindString = Selectors.textAt(node, ENDPOINT_KIND);

    if (!endpointKindString.equals("http")) {
      throw new ModuleConfigurationException("Only http endpoints are supported.");
    }

    // backwards compatibility path
    return new ComponentJsonObject(
        TypeName.parseFrom("io.statefun.endpoints/http"), node.at(ENDPOINT_SPEC));
  }

  private static ComponentJsonObject parseIngressComponentNode(JsonNode node) {
    final TypeName binderTypename = TypeName.parseFrom(Selectors.textAt(node, INGRESS_KIND));

    // backwards compatibility path
    final JsonNode specNode = node.at(INGRESS_SPEC);
    final String idString = Selectors.textAt(node, INGRESS_ID);
    ((ObjectNode) specNode).put("id", idString);

    return new ComponentJsonObject(binderTypename, specNode);
  }

  private static ComponentJsonObject parseEgressComponentNode(JsonNode node) {
    final TypeName binderTypename = TypeName.parseFrom(Selectors.textAt(node, EGRESS_KIND));

    // backwards compatibility path
    final JsonNode specNode = node.at(EGRESS_SPEC);
    final String idString = Selectors.textAt(node, EGRESS_ID);
    ((ObjectNode) specNode).put("id", idString);

    return new ComponentJsonObject(binderTypename, specNode);
  }

  private static void bindComponent(ComponentJsonObject component, Binder moduleBinder) {
    final ExtensionResolver extensionResolver = getExtensionResolver(moduleBinder);
    final ComponentBinder componentBinder =
        extensionResolver.resolveExtension(component.binderTypename(), ComponentBinder.class);
    componentBinder.bind(component, moduleBinder, extensionResolver);
  }

  // TODO expose ExtensionResolver properly once we have more usages
  private static ExtensionResolver getExtensionResolver(Binder binder) {
    return (ExtensionResolver) binder;
  }
}
