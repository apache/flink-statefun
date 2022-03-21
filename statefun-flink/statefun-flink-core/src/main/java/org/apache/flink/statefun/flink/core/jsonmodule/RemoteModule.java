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

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;
import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.flink.core.spi.ExtensionResolver;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RemoteModule implements StatefulFunctionModule {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteModule.class);
  private static final Pattern PLACEHOLDER_REGEX = Pattern.compile("\\$\\{(.*?)\\}");
  private final List<JsonNode> componentNodes;

  RemoteModule(List<JsonNode> componentNodes) {
    this.componentNodes = Objects.requireNonNull(componentNodes);
  }

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder moduleBinder) {
    Map<String, String> systemPropsThenEnvVarsThenGlobalConfig =
        ParameterTool.fromSystemProperties()
            .mergeWith(
                ParameterTool.fromMap(System.getenv())
                    .mergeWith(ParameterTool.fromMap(globalConfiguration)))
            .toMap();
    parseComponentNodes(componentNodes)
        .forEach(
            component ->
                bindComponent(component, moduleBinder, systemPropsThenEnvVarsThenGlobalConfig));
  }

  private static List<ComponentJsonObject> parseComponentNodes(
      Iterable<? extends JsonNode> componentNodes) {
    return StreamSupport.stream(componentNodes.spliterator(), false)
        .filter(node -> !node.isNull())
        .map(ComponentJsonObject::new)
        .collect(Collectors.toList());
  }

  private static void bindComponent(
      ComponentJsonObject component, Binder moduleBinder, Map<String, String> configuration) {

    JsonNode resolvedSpec = valueResolutionFunction(configuration).apply(component.specJsonNode());
    ComponentJsonObject resolvedComponent = new ComponentJsonObject(component.get(), resolvedSpec);

    final ExtensionResolver extensionResolver = getExtensionResolver(moduleBinder);
    final ComponentBinder componentBinder =
        extensionResolver.resolveExtension(
            resolvedComponent.binderTypename(), ComponentBinder.class);
    componentBinder.bind(resolvedComponent, moduleBinder);
  }

  private static Function<JsonNode, JsonNode> valueResolutionFunction(Map<String, String> config) {
    return value -> {
      if (value.isObject()) {
        return resolveObject((ObjectNode) value, config);
      } else if (value.isArray()) {
        return resolveArray((ArrayNode) value, config);
      } else if (value.isValueNode()) {
        return resolveValueNode((ValueNode) value, config);
      }

      LOG.warn(
          "Unrecognised type (not in: object, array, value). Skipping ${placeholder} resolution for that node.");
      return value;
    };
  }

  private static Function<Map.Entry<String, JsonNode>, AbstractMap.SimpleEntry<String, JsonNode>>
      keyValueResolutionFunction(Map<String, String> config) {
    return fieldNameValuePair ->
        new AbstractMap.SimpleEntry<>(
            fieldNameValuePair.getKey(),
            valueResolutionFunction(config).apply(fieldNameValuePair.getValue()));
  }

  private static ValueNode resolveValueNode(ValueNode node, Map<String, String> config) {
    StringBuffer stringBuffer = new StringBuffer();
    Matcher placeholderMatcher = PLACEHOLDER_REGEX.matcher(node.asText());
    boolean placeholderReplaced = false;

    while (placeholderMatcher.find()) {
      if (config.containsKey(placeholderMatcher.group(1))) {
        placeholderMatcher.appendReplacement(stringBuffer, config.get(placeholderMatcher.group(1)));
        placeholderReplaced = true;
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Could not resolve placeholder '%s'. An entry for this key was not found in the configuration.",
                node.asText()));
      }
    }

    if (placeholderReplaced) {
      placeholderMatcher.appendTail(stringBuffer);
      return new TextNode(stringBuffer.toString());
    }

    return node;
  }

  private static ObjectNode resolveObject(ObjectNode node, Map<String, String> config) {
    return getFieldStream(node)
        .map(keyValueResolutionFunction(config))
        .reduce(
            new ObjectNode(JsonNodeFactory.instance),
            (accumulatedObjectNode, resolvedFieldNameValueTuple) -> {
              accumulatedObjectNode.put(
                  resolvedFieldNameValueTuple.getKey(), resolvedFieldNameValueTuple.getValue());
              return accumulatedObjectNode;
            },
            (objectNode1, objectNode2) -> {
              throw new NotImplementedException("This reduce is not used with parallel streams");
            });
  }

  private static ArrayNode resolveArray(ArrayNode node, Map<String, String> config) {
    return getElementStream(node)
        .map(valueResolutionFunction(config))
        .reduce(
            new ArrayNode(JsonNodeFactory.instance),
            (accumulatedArrayNode, resolvedValue) -> {
              accumulatedArrayNode.add(resolvedValue);
              return accumulatedArrayNode;
            },
            (arrayNode1, arrayNode2) -> {
              throw new NotImplementedException("This reduce is not used with parallel streams");
            });
  }

  private static Stream<Map.Entry<String, JsonNode>> getFieldStream(ObjectNode node) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(node.fields(), 0), false);
  }

  private static Stream<JsonNode> getElementStream(ArrayNode node) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(node.elements(), 0), false);
  }
}
