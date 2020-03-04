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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.statefun.flink.common.ResourceLocator;
import org.apache.flink.statefun.flink.core.spi.Constants;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public final class JsonServiceLoader {

  public static Iterable<StatefulFunctionModule> load() {
    ObjectMapper mapper = mapper();

    Iterable<URL> namedResources =
        ResourceLocator.findNamedResources("classpath:" + Constants.STATEFUL_FUNCTIONS_MODULE_NAME);

    return StreamSupport.stream(namedResources.spliterator(), false)
        .map(moduleUrl -> fromUrl(mapper, moduleUrl))
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  static StatefulFunctionModule fromUrl(ObjectMapper mapper, URL moduleUrl) {
    try {
      JsonNode root = readAndValidateModuleTree(mapper, moduleUrl);
      return JsonModuleFactory.create(root, moduleUrl);
    } catch (Throwable t) {
      throw new RuntimeException("Failed loading a module at " + moduleUrl, t);
    }
  }

  /**
   * Read a {@code StatefulFunction} module definition.
   *
   * <p>A valid resource module definition has to contain the following sections:
   *
   * <ul>
   *   <li>meta - contains the metadata associated with this module, such as its type.
   *   <li>spec - a specification of the module. i.e. the definied functions, routers etc'.
   * </ul>
   *
   * <p>If any of these sections are missing, this would be considered an invalid module definition,
   * in addition a type is a mandatory field of a module spec.
   */
  private static JsonNode readAndValidateModuleTree(ObjectMapper mapper, URL moduleYamlFile)
      throws IOException {
    JsonNode root = mapper.readTree(moduleYamlFile);
    validateMeta(moduleYamlFile, root);
    validateSpec(moduleYamlFile, root);
    return root;
  }

  private static void validateMeta(URL moduleYamlFile, JsonNode root) {
    JsonNode typeNode = root.at(Pointers.MODULE_META_TYPE);
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

  private static void validateSpec(URL moduleYamlFile, JsonNode root) {
    if (root.at(Pointers.MODULE_SPEC).isMissingNode()) {
      throw new IllegalStateException("A module without a spec at " + moduleYamlFile);
    }
  }

  @VisibleForTesting
  static ObjectMapper mapper() {
    return new ObjectMapper(new YAMLFactory());
  }
}
