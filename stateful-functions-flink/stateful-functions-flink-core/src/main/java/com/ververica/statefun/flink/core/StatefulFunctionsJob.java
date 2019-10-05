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

package com.ververica.statefun.flink.core;

import com.ververica.statefun.flink.core.common.ConfigurationUtil;
import com.ververica.statefun.flink.core.translation.FlinkUniverse;
import java.util.Objects;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StatefulFunctionsJob {

  public static void main(String... args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    Configuration configuration = parameterTool.getConfiguration();

    main(configuration);
  }

  public static void main(Configuration configuration) throws Exception {
    Objects.requireNonNull(configuration);

    setDefaultContextClassLoaderIfAbsent();
    setDefaultProviderIfAbsent(
        configuration, new StatefulFunctionsUniverses.ClassPathUniverseProvider());

    final StatefulFunctionsUniverse statefulFunctionsUniverse =
        StatefulFunctionsUniverses.get(
            Thread.currentThread().getContextClassLoader(), configuration);

    final StatefulFunctionsUniverseValidator statefulFunctionsUniverseValidator =
        new StatefulFunctionsUniverseValidator();
    statefulFunctionsUniverseValidator.validate(statefulFunctionsUniverse);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    setDefaultConfiguration(configuration, env);

    FlinkUniverse flinkUniverse = new FlinkUniverse(statefulFunctionsUniverse);
    flinkUniverse.configure(env);

    String jobName = configuration.getValue(StatefulFunctionsJobConstants.FLINK_JOB_NAME);
    env.execute(jobName);
  }

  private static void setDefaultConfiguration(
      Configuration configuration, StreamExecutionEnvironment env) {
    env.getConfig().setGlobalJobParameters(configuration);
    env.getConfig().enableObjectReuse();
    final long checkpointingInterval =
        configuration.getLong(StatefulFunctionsJobConstants.CHECKPOINTING_INTERVAL);
    if (checkpointingInterval > 0) {
      env.enableCheckpointing(checkpointingInterval);
    }
  }

  private static void setDefaultContextClassLoaderIfAbsent() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      Thread.currentThread().setContextClassLoader(StatefulFunctionsJob.class.getClassLoader());
    }
  }

  private static void setDefaultProviderIfAbsent(
      Configuration configuration, StatefulFunctionsUniverseProvider provider) {
    if (!configuration.contains(
        StatefulFunctionsJobConstants.STATEFUL_FUNCTIONS_UNIVERSE_INITIALIZER_CLASS_BYTES)) {
      ConfigurationUtil.storeSerializedInstance(
          configuration,
          StatefulFunctionsJobConstants.STATEFUL_FUNCTIONS_UNIVERSE_INITIALIZER_CLASS_BYTES,
          provider);
    }
  }
}
