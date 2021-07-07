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

package org.apache.flink.statefun.sdk.spi;

import java.util.Map;
import org.apache.flink.statefun.sdk.TypeName;

/**
 * A module that binds multiple extension objects to the Stateful Functions application. Each
 * extension is uniquely identified by a {@link TypeName}.
 */
public interface ExtensionModule {

  /**
   * This method binds multiple extension objects to the Stateful Functions application.
   *
   * @param globalConfigurations global configuration of the Stateful Functions application.
   * @param binder binder for binding extensions.
   */
  void configure(Map<String, String> globalConfigurations, Binder binder);

  interface Binder {
    <T> void bindExtension(TypeName typeName, T extension);
  }
}
