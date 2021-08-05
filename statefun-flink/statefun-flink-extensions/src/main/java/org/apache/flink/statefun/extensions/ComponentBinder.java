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

package org.apache.flink.statefun.extensions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * A {@link ComponentBinder} binds {@link ComponentJsonObject}s to a remote module. It parses the
 * specifications of a given component, resolves them into application entities, such as function
 * providers, ingresses, or egresses, and then binds the entities to the module.
 */
@PublicEvolving
public interface ComponentBinder {

  /**
   * Bind a {@link ComponentJsonObject} to an underlying remote module through the provided module
   * binder.
   *
   * @param component the component to parse and bind.
   * @param remoteModuleBinder the binder to use to bind application entities to the underlying
   *     remote module.
   * @param extensionResolver resolver for extensions existing in the application universe.
   */
  void bind(
      ComponentJsonObject component,
      StatefulFunctionModule.Binder remoteModuleBinder,
      ExtensionResolver extensionResolver);
}
