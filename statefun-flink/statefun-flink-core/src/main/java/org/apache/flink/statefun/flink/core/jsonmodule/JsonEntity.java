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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.core.spi.ExtensionResolver;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule.Binder;

/**
 * A {@link JsonEntity} represents a section within a {@link JsonModule} that should be parsed into
 * application entity specs (of functions, routers, ingresses, egresses, etc.) and bind to the
 * module.
 */
interface JsonEntity {

  /**
   * Parse the module spec node, and bind result specs to the module.
   *
   * @param binder used to bind specs to the module.
   * @param moduleSpecNode the root module spec node.
   * @param formatVersion the format version of the module spec.
   */
  void bind(
      Binder binder,
      ExtensionResolver extensionResolver,
      JsonNode moduleSpecNode,
      FormatVersion formatVersion);
}
