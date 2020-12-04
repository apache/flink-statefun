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

import static java.lang.String.format;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

final class JsonModule implements StatefulFunctionModule {

  /** Entities in the JSON moduleSpecNode that should be parsed and bound to the module. */
  private static final List<JsonEntity> ENTITIES =
      Arrays.asList(
          new FunctionEndpointJsonEntity(),
          new IngressJsonEntity(),
          new RouterJsonEntity(),
          new EgressJsonEntity());

  private final JsonNode moduleSpecNode;
  private final FormatVersion formatVersion;
  private final URL moduleUrl;

  public JsonModule(JsonNode moduleSpecNode, FormatVersion formatVersion, URL moduleUrl) {
    this.moduleSpecNode = Objects.requireNonNull(moduleSpecNode);
    this.formatVersion = Objects.requireNonNull(formatVersion);
    this.moduleUrl = Objects.requireNonNull(moduleUrl);
  }

  public void configure(Map<String, String> conf, Binder binder) {
    try {
      ENTITIES.forEach(jsonEntity -> jsonEntity.bind(binder, moduleSpecNode, formatVersion));
    } catch (Throwable t) {
      throw new ModuleConfigurationException(
          format("Error while parsing module at %s", moduleUrl), t);
    }
  }
}
