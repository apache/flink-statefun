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
package org.apache.flink.statefun.flink.core;

import org.apache.flink.statefun.flink.core.spi.Modules;
import org.apache.flink.util.Preconditions;

public final class StatefulFunctionsUniverses {

  public static StatefulFunctionsUniverse get(
      ClassLoader classLoader, StatefulFunctionsConfig configuration) {
    Preconditions.checkState(classLoader != null, "The class loader was not set.");
    Preconditions.checkState(configuration != null, "The configuration was not set.");

    StatefulFunctionsUniverseProvider provider = configuration.getProvider(classLoader);

    return provider.get(classLoader, configuration);
  }

  static final class ClassPathUniverseProvider implements StatefulFunctionsUniverseProvider {

    private static final long serialVersionUID = 1;

    @Override
    public StatefulFunctionsUniverse get(
        ClassLoader classLoader, StatefulFunctionsConfig configuration) {
      Modules modules = Modules.loadFromClassPath();
      return modules.createStatefulFunctionsUniverse(configuration);
    }
  }
}
