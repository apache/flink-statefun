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

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import org.apache.flink.statefun.flink.common.ResourceLocator;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public final class JsonServiceLoader {

  // =======================================================================
  //  Json pointers for backwards compatibility with legacy format v3.0
  // =======================================================================
  private static final JsonPointer FORMAT_VERSION = JsonPointer.compile("/version");
  private static final JsonPointer MODULE_SPEC = JsonPointer.compile("/module/spec");
  private static final JsonPointer MODULE_META_TYPE = JsonPointer.compile("/module/meta/type");

  public static Iterable<StatefulFunctionModule> load(String moduleName) {
    ObjectMapper mapper = mapper();

    Iterable<URL> namedResources = ResourceLocator.findResources(moduleName);

    return StreamSupport.stream(namedResources.spliterator(), false)
        .map(moduleUrl -> fromUrl(mapper, moduleUrl))
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  static StatefulFunctionModule fromUrl(ObjectMapper mapper, URL moduleUrl) {
    try {
      final List<JsonNode> allComponentNodes = readAllComponentNodes(mapper, moduleUrl);

      if (isLegacySingleRootFormat(allComponentNodes)) {
        return createLegacyRemoteModule(allComponentNodes.get(0), moduleUrl);
      }
      return new RemoteModule(allComponentNodes);
    } catch (Throwable t) {
      throw new RuntimeException("Failed loading a module at " + moduleUrl, t);
    }
  }

  /**
   * Read a {@code StatefulFunction} module definition.
   *
   * <p>A valid resource module definition has to contain the metadata associated with this module,
   * such as its type.
   */
  private static List<JsonNode> readAllComponentNodes(ObjectMapper mapper, URL moduleYamlFile)
      throws IOException {
    YAMLFactory yaml = new YAMLFactory();

    YAMLParser yamlParser = yaml.createParser(moduleYamlFile);
    return mapper.readValues(yamlParser, JsonNode.class).readAll();
  }

  private static void validateMeta(URL moduleYamlFile, JsonNode root) {
    JsonNode typeNode = root.at(MODULE_META_TYPE);
    if (typeNode.isMissingNode()) {
      throw new IllegalStateException("Unable to find a module type in " + moduleYamlFile);
    }
    if (!typeNode.asText().equalsIgnoreCase(ModuleType.REMOTE.name())) {
      throw new IllegalStateException(
          "Unknown module type "
              + typeNode.asText()
              + ", currently supported: "
              + ModuleType.REMOTE);
    }
  }

  private static JsonNode requireValidModuleSpecNode(URL moduleYamlFile, JsonNode root) {
    final JsonNode moduleSpecNode = root.at(MODULE_SPEC);

    if (moduleSpecNode.isMissingNode()) {
      throw new IllegalStateException("A module without a spec at " + moduleYamlFile);
    }

    return moduleSpecNode;
  }

  private static boolean isLegacySingleRootFormat(List<JsonNode> allComponentNodes) {
    if (allComponentNodes.size() == 1) {
      final JsonNode singleRootNode = allComponentNodes.get(0);
      return hasLegacyFormatVersionField(singleRootNode);
    }
    return false;
  }

  private static boolean hasLegacyFormatVersionField(JsonNode singleRootNode) {
    return Selectors.optionalTextAt(singleRootNode, FORMAT_VERSION).isPresent();
  }

  private static StatefulFunctionModule createLegacyRemoteModule(
      JsonNode singleRootNode, URL moduleUrl) {
    validateMeta(moduleUrl, singleRootNode);
    final FormatVersion version = requireValidFormatVersion(singleRootNode);
    switch (version) {
      case v3_0:
        return new LegacyRemoteModuleV30(requireValidModuleSpecNode(moduleUrl, singleRootNode));
      default:
        throw new IllegalStateException("Unrecognized format version: " + version);
    }
  }

  private static FormatVersion requireValidFormatVersion(JsonNode root) {
    final String formatVersionStr = Selectors.textAt(root, FORMAT_VERSION);
    final FormatVersion formatVersion = FormatVersion.fromString(formatVersionStr);
    if (formatVersion.compareTo(FormatVersion.v3_0) < 0) {
      throw new IllegalArgumentException(
          "Only format versions higher than or equal to 3.0 is supported. Was version "
              + formatVersion
              + ".");
    }
    return formatVersion;
  }

  @VisibleForTesting
  static ObjectMapper mapper() {
    return new ObjectMapper(new YAMLFactory());
  }
}
