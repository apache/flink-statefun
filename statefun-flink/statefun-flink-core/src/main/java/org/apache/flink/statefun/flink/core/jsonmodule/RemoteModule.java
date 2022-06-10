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

import static org.apache.flink.statefun.flink.core.spi.ExtensionResolverAccessor.getExtensionResolver;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.flink.core.spi.ExtensionResolver;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public final class RemoteModule implements StatefulFunctionModule {

  private final List<JsonNode> componentNodes;

  RemoteModule(List<JsonNode> componentNodes) {
    this.componentNodes = Objects.requireNonNull(componentNodes);
  }

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder moduleBinder) {
    parseComponentNodes(componentNodes)
        .forEach(component -> bindComponent(component, moduleBinder));
  }

  private static List<ComponentJsonObject> parseComponentNodes(
      Iterable<? extends JsonNode> componentNodes) {
    return StreamSupport.stream(componentNodes.spliterator(), false)
        .filter(node -> !node.isNull())
        .map(ComponentJsonObject::new)
        .collect(Collectors.toList());
  }

  private static void bindComponent(ComponentJsonObject component, Binder moduleBinder) {
    final ExtensionResolver extensionResolver = getExtensionResolver(moduleBinder);
    final ComponentBinder componentBinder =
        extensionResolver.resolveExtension(component.binderTypename(), ComponentBinder.class);
    componentBinder.bind(component, moduleBinder);
  }
}
