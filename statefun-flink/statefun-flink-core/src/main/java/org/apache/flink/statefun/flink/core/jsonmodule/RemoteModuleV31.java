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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.flink.common.extensions.ComponentBinder;
import org.apache.flink.statefun.flink.common.extensions.ExtensionResolver;
import org.apache.flink.statefun.flink.common.json.ModuleComponent;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public final class RemoteModuleV31 implements StatefulFunctionModule {

  private static final ObjectMapper COMPONENT_OBJ_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static final JsonPointer ENDPOINTS = JsonPointer.compile("/endpoints");
  private static final JsonPointer INGRESSES = JsonPointer.compile("/ingresses");
  private static final JsonPointer EGRESSES = JsonPointer.compile("/egresses");

  private final JsonNode moduleSpecNode;

  RemoteModuleV31(JsonNode moduleSpecNode) {
    this.moduleSpecNode = Objects.requireNonNull(moduleSpecNode);
  }

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder moduleBinder) {
    components(moduleSpecNode).forEach(component -> bindComponent(component, moduleBinder));
  }

  private static Iterable<ModuleComponent> components(JsonNode moduleSpecNode) {
    final List<ModuleComponent> endpointComponents =
        parseComponentNodes(Selectors.listAt(moduleSpecNode, ENDPOINTS));
    final List<ModuleComponent> ingressComponents =
        parseComponentNodes(Selectors.listAt(moduleSpecNode, EGRESSES));
    final List<ModuleComponent> egressComponents =
        parseComponentNodes(Selectors.listAt(moduleSpecNode, INGRESSES));

    final List<ModuleComponent> allComponents = new ArrayList<>();
    allComponents.addAll(endpointComponents);
    allComponents.addAll(ingressComponents);
    allComponents.addAll(egressComponents);
    return allComponents;
  }

  private static List<ModuleComponent> parseComponentNodes(
      Iterable<? extends JsonNode> componentNodes) {
    return StreamSupport.stream(componentNodes.spliterator(), false)
        .map(RemoteModuleV31::parseComponentNode)
        .collect(Collectors.toList());
  }

  private static ModuleComponent parseComponentNode(JsonNode componentNode) {
    try {
      return COMPONENT_OBJ_MAPPER.treeToValue(componentNode, ModuleComponent.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static void bindComponent(ModuleComponent component, Binder moduleBinder) {
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
