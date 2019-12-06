/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.flink.core.spi;

import com.ververica.statefun.flink.core.StatefulFunctionsJobConstants;
import com.ververica.statefun.flink.core.StatefulFunctionsUniverse;
import com.ververica.statefun.flink.core.common.SetContextClassLoader;
import com.ververica.statefun.flink.core.jsonmodule.JsonServiceLoader;
import com.ververica.statefun.flink.core.message.MessageFactoryType;
import com.ververica.statefun.flink.io.spi.FlinkIoModule;
import com.ververica.statefun.sdk.spi.StatefulFunctionModule;
import java.util.*;
import org.apache.flink.configuration.Configuration;

public final class Modules {
  private final List<FlinkIoModule> ioModules;
  private final List<StatefulFunctionModule> statefulFunctionModules;

  private Modules(
      List<FlinkIoModule> ioModules, List<StatefulFunctionModule> statefulFunctionModules) {
    this.ioModules = ioModules;
    this.statefulFunctionModules = statefulFunctionModules;
  }

  public static Modules loadFromClassPath() {
    List<StatefulFunctionModule> statefulFunctionModules = new ArrayList<>();
    List<FlinkIoModule> ioModules = new ArrayList<>();

    for (StatefulFunctionModule provider : ServiceLoader.load(StatefulFunctionModule.class)) {
      statefulFunctionModules.add(provider);
    }
    for (StatefulFunctionModule provider : JsonServiceLoader.load()) {
      statefulFunctionModules.add(provider);
    }
    for (FlinkIoModule provider : ServiceLoader.load(FlinkIoModule.class)) {
      ioModules.add(provider);
    }
    return new Modules(ioModules, statefulFunctionModules);
  }

  public StatefulFunctionsUniverse createStatefulFunctionsUniverse(Configuration configuration) {
    MessageFactoryType factoryType =
        configuration.getEnum(
            MessageFactoryType.class, StatefulFunctionsJobConstants.USER_MESSAGE_SERIALIZER);

    StatefulFunctionsUniverse universe = new StatefulFunctionsUniverse(factoryType);

    final Map<String, String> globalConfiguration =
        Collections.unmodifiableMap(configuration.toMap());

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
