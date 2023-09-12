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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import okhttp3.OkHttpClient;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.translation.FlinkUniverse;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FlinkUserCodeClassLoader;
import org.apache.flink.util.FlinkUserCodeClassLoaders;

public class StatefulFunctionsJob {

  private static final AtomicInteger FEEDBACK_INVOCATION_ID_SEQ = new AtomicInteger();

  public static void main(String... args) throws Exception {
    ParameterTool argsParameterTool = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Configuration flinkConfig = FlinkConfigExtractor.reflectivelyExtractFromEnv(env);

    StatefulFunctionsConfig stateFunConfig =
        StatefulFunctionsConfig.fromFlinkConfiguration(
            ParameterTool.fromMap(flinkConfig.toMap())
                .mergeWith(argsParameterTool)
                .getConfiguration());

    stateFunConfig.addAllGlobalConfigurations(argsParameterTool.toMap());
    stateFunConfig.setProvider(new StatefulFunctionsUniverses.ClassPathUniverseProvider());

    StatefulFunctionsConfigValidator.validate(stateFunConfig.isEmbedded(), flinkConfig);

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

    // Enable fine-grained logging for OkHttp to get details about unclosed connections when they
    // occur
    Logger.getLogger(OkHttpClient.class.getName()).setLevel(Level.FINE);

    final StatefulFunctionsUniverse statefulFunctionsUniverse =
        StatefulFunctionsUniverses.get(
            Thread.currentThread().getContextClassLoader(), stateFunConfig);

    final StatefulFunctionsUniverseValidator statefulFunctionsUniverseValidator =
        new StatefulFunctionsUniverseValidator();
    statefulFunctionsUniverseValidator.validate(statefulFunctionsUniverse);

    FeedbackKey<Message> feedbackKey =
        new FeedbackKey<>("statefun-pipeline", FEEDBACK_INVOCATION_ID_SEQ.incrementAndGet());
    FlinkUniverse flinkUniverse =
        new FlinkUniverse(feedbackKey, stateFunConfig, statefulFunctionsUniverse);
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
              FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER,
              false);
      Thread.currentThread().setContextClassLoader(flinkClassLoader);
    }
  }
}
