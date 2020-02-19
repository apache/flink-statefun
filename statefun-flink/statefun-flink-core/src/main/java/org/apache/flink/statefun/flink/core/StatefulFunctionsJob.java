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

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.statefun.flink.core.translation.FlinkUniverse;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@SuppressWarnings("JavaReflectionMemberAccess")
public class StatefulFunctionsJob {

  private static final String CONFIG_DIRECTORY_FALLBACK_1 = "../conf";
  private static final String CONFIG_DIRECTORY_FALLBACK_2 = "conf";

  public static void main(String... args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    Map<String, String> globalConfigurations = parameterTool.toMap();

    String configDirectory = getConfigurationDirectoryFromEnv();
    Configuration flinkConf = GlobalConfiguration.loadConfiguration(configDirectory);
    StatefulFunctionsConfig stateFunConfig = new StatefulFunctionsConfig(flinkConf);
    stateFunConfig.setGlobalConfigurations(globalConfigurations);
    stateFunConfig.setProvider(new StatefulFunctionsUniverses.ClassPathUniverseProvider());

    main(stateFunConfig, new Configuration());
  }

  public static void main(StatefulFunctionsConfig stateFunConfig, Configuration flinkConf)
      throws Exception {
    Objects.requireNonNull(stateFunConfig);
    Objects.requireNonNull(flinkConf);

    setDefaultContextClassLoaderIfAbsent();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.configure(flinkConf, Thread.currentThread().getContextClassLoader());

    env.getConfig().enableObjectReuse();

    final StatefulFunctionsUniverse statefulFunctionsUniverse =
        StatefulFunctionsUniverses.get(
            Thread.currentThread().getContextClassLoader(), stateFunConfig);

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
              new URL[0], StatefulFunctionsJob.class.getClassLoader());
      Thread.currentThread().setContextClassLoader(flinkClassLoader);
    }
  }

  /**
   * Finds the location of the flink-conf. The fallback keys are required to find the configuration
   * in non-image based deployments; (i.e., anything using the flink cli).
   *
   * <p>Taken from org.apache.flink.client.cli.CliFrontend
   */
  private static String getConfigurationDirectoryFromEnv() {
    String location = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);

    if (location != null) {
      if (new File(location).exists()) {
        return location;
      } else {
        throw new RuntimeException(
            "The configuration directory '"
                + location
                + "', specified in the '"
                + ConfigConstants.ENV_FLINK_CONF_DIR
                + "' environment variable, does not exist.");
      }
    } else if (new File(CONFIG_DIRECTORY_FALLBACK_1).exists()) {
      location = CONFIG_DIRECTORY_FALLBACK_1;
    } else if (new File(CONFIG_DIRECTORY_FALLBACK_2).exists()) {
      location = CONFIG_DIRECTORY_FALLBACK_2;
    } else {
      throw new RuntimeException(
          "The configuration directory was not specified. "
              + "Please specify the directory containing the configuration file through the '"
              + ConfigConstants.ENV_FLINK_CONF_DIR
              + "' environment variable.");
    }
    return location;
  }
}
