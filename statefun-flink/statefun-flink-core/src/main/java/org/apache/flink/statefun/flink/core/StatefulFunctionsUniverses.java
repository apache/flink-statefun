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

import static java.util.Collections.synchronizedMap;

import java.util.Map;
import java.util.WeakHashMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.statefun.flink.core.common.ConfigurationUtil;
import org.apache.flink.statefun.flink.core.spi.Modules;
import org.apache.flink.util.Preconditions;

public final class StatefulFunctionsUniverses {

  private static final Map<ClassLoader, StatefulFunctionsUniverse> universes =
      synchronizedMap(new WeakHashMap<>());

  public static StatefulFunctionsUniverse get(
      ClassLoader classLoader, Configuration configuration) {
    Preconditions.checkState(classLoader != null, "The class loader was not set.");
    Preconditions.checkState(configuration != null, "The configuration was not set.");

    return universes.computeIfAbsent(
        classLoader, cl -> initializeFromConfiguration(cl, configuration));
  }

  private static StatefulFunctionsUniverse initializeFromConfiguration(
      ClassLoader cl, Configuration configuration) {

    StatefulFunctionsUniverseProvider provider =
        ConfigurationUtil.getSerializedInstance(
            cl,
            configuration,
            StatefulFunctionsJobConstants.STATEFUL_FUNCTIONS_UNIVERSE_INITIALIZER_CLASS_BYTES);

    return provider.get(cl, configuration);
  }

  static final class ClassPathUniverseProvider implements StatefulFunctionsUniverseProvider {

    private static final long serialVersionUID = 1;

    @Override
    public StatefulFunctionsUniverse get(ClassLoader classLoader, Configuration configuration) {
      Modules modules = Modules.loadFromClassPath();
      return modules.createStatefulFunctionsUniverse(configuration);
    }
  }
}
