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
package org.apache.flink.statefun.flink.core.spi;

import java.util.*;
import org.apache.flink.statefun.extensions.ExtensionModule;
import org.apache.flink.statefun.flink.common.SetContextClassLoader;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.jsonmodule.JsonServiceLoader;
import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.io.spi.FlinkIoModule;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public final class Modules {
  private final List<ExtensionModule> extensionModules;
  private final List<FlinkIoModule> ioModules;
  private final List<StatefulFunctionModule> statefulFunctionModules;

  private Modules(
      List<ExtensionModule> extensionModules,
      List<FlinkIoModule> ioModules,
      List<StatefulFunctionModule> statefulFunctionModules) {
    this.extensionModules = extensionModules;
    this.ioModules = ioModules;
    this.statefulFunctionModules = statefulFunctionModules;
  }

  public static Modules loadFromClassPath() {
    List<StatefulFunctionModule> statefulFunctionModules = new ArrayList<>();
    List<FlinkIoModule> ioModules = new ArrayList<>();
    List<ExtensionModule> extensionModules = new ArrayList<>();

    for (ExtensionModule extensionModule : ServiceLoader.load(ExtensionModule.class)) {
      extensionModules.add(extensionModule);
    }
    for (StatefulFunctionModule provider : ServiceLoader.load(StatefulFunctionModule.class)) {
      statefulFunctionModules.add(provider);
    }
    for (StatefulFunctionModule provider : JsonServiceLoader.load()) {
      statefulFunctionModules.add(provider);
    }
    for (FlinkIoModule provider : ServiceLoader.load(FlinkIoModule.class)) {
      ioModules.add(provider);
    }
    return new Modules(extensionModules, ioModules, statefulFunctionModules);
  }

  public StatefulFunctionsUniverse createStatefulFunctionsUniverse(
      StatefulFunctionsConfig configuration) {
    MessageFactoryKey factoryKey = configuration.getFactoryKey();

    StatefulFunctionsUniverse universe = new StatefulFunctionsUniverse(factoryKey);

    final Map<String, String> globalConfiguration = configuration.getGlobalConfigurations();

    // it is important to bind and configure the extension modules first, since
    // other modules (IO and functions) may use extensions already.
    for (ExtensionModule module : extensionModules) {
      try (SetContextClassLoader ignored = new SetContextClassLoader(module)) {
        module.configure(globalConfiguration, universe);
      }
    }
    for (FlinkIoModule module : ioModules) {
      try (SetContextClassLoader ignored = new SetContextClassLoader(module)) {
        module.configure(globalConfiguration, universe);
      }
    }
    for (StatefulFunctionModule module : statefulFunctionModules) {
      try (SetContextClassLoader ignored = new SetContextClassLoader(module)) {
        module.configure(globalConfiguration, universe);
      }
    }

    return universe;
  }
}
