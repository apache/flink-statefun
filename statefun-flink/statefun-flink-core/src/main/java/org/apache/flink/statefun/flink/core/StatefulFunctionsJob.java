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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.statefun.flink.core.classloader.ModuleClassLoader;
import org.apache.flink.statefun.flink.core.translation.FlinkUniverse;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FlinkUserCodeClassLoader;

public class StatefulFunctionsJob {

  public static void main(String... args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    Map<String, String> globalConfigurations = parameterTool.toMap();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StatefulFunctionsConfig stateFunConfig = StatefulFunctionsConfig.fromEnvironment(env);
    stateFunConfig.enableModuleClassPath();
    stateFunConfig.addAllGlobalConfigurations(globalConfigurations);
    stateFunConfig.setProvider(new StatefulFunctionsUniverses.ClassPathUniverseProvider());

    main(env, stateFunConfig);
  }

  /**
   * The main entry point for executing a stateful functions application.
   *
   * @param env The StreamExecutionEnvironment under which the application will be bound.
   * @param stateFunConfig The stateful function specific configurations for the deployment.
   */
  public static void main(StreamExecutionEnvironment env, StatefulFunctionsConfig stateFunConfig)
      throws Exception {
    Objects.requireNonNull(env);
    Objects.requireNonNull(stateFunConfig);

    setDefaultContextClassLoaderIfAbsent();

    env.getConfig().enableObjectReuse();

    final ClassLoader moduleClassLoader =
        ModuleClassLoader.createModuleClassLoader(
            stateFunConfig, Thread.currentThread().getContextClassLoader());
    final StatefulFunctionsUniverse statefulFunctionsUniverse =
        StatefulFunctionsUniverses.get(moduleClassLoader, stateFunConfig);

    final StatefulFunctionsUniverseValidator statefulFunctionsUniverseValidator =
        new StatefulFunctionsUniverseValidator();
    statefulFunctionsUniverseValidator.validate(statefulFunctionsUniverse);

    FlinkUniverse flinkUniverse = new FlinkUniverse(statefulFunctionsUniverse, stateFunConfig);
    flinkUniverse.configure(env);

    env.execute(stateFunConfig.getFlinkJobName());
  }

  private static void setDefaultContextClassLoaderIfAbsent() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      URLClassLoader flinkClassLoader =
          FlinkUserCodeClassLoaders.parentFirst(
              new URL[0],
              StatefulFunctionsJob.class.getClassLoader(),
              FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER);
      Thread.currentThread().setContextClassLoader(flinkClassLoader);
    }
  }
}
