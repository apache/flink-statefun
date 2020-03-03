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

import static org.apache.flink.configuration.description.TextElement.code;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.statefun.flink.core.exceptions.StatefulFunctionsInvalidConfigException;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamPlanEnvironment;
import org.apache.flink.util.InstantiationUtil;

/** Configuration that captures all stateful function related settings. */
@SuppressWarnings("WeakerAccess")
public class StatefulFunctionsConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String MODULE_CONFIG_PREFIX = "statefun.module.global-config.";

  // This configuration option exists for the documentation generator
  @SuppressWarnings("unused")
  public static final ConfigOption<String> MODULE_GLOBAL_DEFAULT =
      ConfigOptions.key(MODULE_CONFIG_PREFIX + "<KEY>")
          .stringType()
          .noDefaultValue()
          .withDescription(
              Description.builder()
                  .text(
                      "Adds the given key/value pair to the Stateful Functions global configuration.")
                  .text(
                      "These values will be available via the `globalConfigurations` parameter of StatefulFunctionModule#configure.")
                  .linebreak()
                  .text(
                      "Only the key <KEY> and value are added to the configuration. If the key/value pairs")
                  .list(
                      code(MODULE_CONFIG_PREFIX + "key1: value1"),
                      code(MODULE_CONFIG_PREFIX + "key2: value2"))
                  .text("are set, then the map")
                  .list(code("key1: value1"), code("key2: value2"))
                  .text("will be made available to your module at runtime.")
                  .build());

  public static final ConfigOption<MessageFactoryType> USER_MESSAGE_SERIALIZER =
      ConfigOptions.key("statefun.message.serializer")
          .enumType(MessageFactoryType.class)
          .defaultValue(MessageFactoryType.WITH_PROTOBUF_PAYLOADS)
          .withDescription("The serializer to use for on the wire messages.");

  public static final ConfigOption<String> FLINK_JOB_NAME =
      ConfigOptions.key("statefun.flink-job-name")
          .stringType()
          .defaultValue("StatefulFunctions")
          .withDescription("The name to display at the Flink-UI");

  public static final ConfigOption<MemorySize> TOTAL_MEMORY_USED_FOR_FEEDBACK_CHECKPOINTING =
      ConfigOptions.key("statefun.feedback.memory.size")
          .memoryType()
          .defaultValue(MemorySize.ofMebiBytes(32))
          .withDescription(
              "The number of bytes to use for in memory buffering of the feedback channel, before spilling to disk.");

  public static final ConfigOption<Integer> ASYNC_MAX_OPERATIONS_PER_TASK =
      ConfigOptions.key("statefun.async.max-per-task")
          .intType()
          .defaultValue(10_000_000)
          .withDescription(
              "The max number of async operations per task before backpressure is applied.");

  /**
   * Creates a new {@link StatefulFunctionsConfig} based on the default configurations in the
   * current environment set via the {@code flink-conf.yaml}.
   */
  public static StatefulFunctionsConfig fromEnvironment(StreamExecutionEnvironment env) {
    Configuration configuration = getConfiguration(env);
    return new StatefulFunctionsConfig(configuration);
  }

  private static Configuration getConfiguration(StreamExecutionEnvironment env) {
    if (env instanceof StreamPlanEnvironment) {
      return GlobalConfiguration.loadConfiguration();
    }
    try {
      Method getConfiguration =
          StreamExecutionEnvironment.class.getDeclaredMethod("getConfiguration");
      getConfiguration.setAccessible(true);
      return (Configuration) getConfiguration.invoke(env);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(
          "Failed to acquire the Flink configuration from the current environment", e);
    }
  }

  private MessageFactoryType factoryType;

  private String flinkJobName;

  private byte[] universeInitializerClassBytes;

  private MemorySize feedbackBufferSize;

  private int maxAsyncOperationsPerTask;

  private Map<String, String> globalConfigurations = new HashMap<>();

  /**
   * Create a new configuration object based on the values set in flink-conf.
   *
   * @param configuration a configuration to read the values from
   */
  public StatefulFunctionsConfig(Configuration configuration) {
    validateStrictlyRequiredConfigs(configuration);

    this.factoryType = configuration.get(USER_MESSAGE_SERIALIZER);
    this.flinkJobName = configuration.get(FLINK_JOB_NAME);
    this.feedbackBufferSize = configuration.get(TOTAL_MEMORY_USED_FOR_FEEDBACK_CHECKPOINTING);
    this.maxAsyncOperationsPerTask = configuration.get(ASYNC_MAX_OPERATIONS_PER_TASK);

    for (String key : configuration.keySet()) {
      if (key.startsWith(MODULE_CONFIG_PREFIX)) {
        String value = configuration.get(ConfigOptions.key(key).stringType().noDefaultValue());
        String userKey = key.substring(MODULE_CONFIG_PREFIX.length());
        globalConfigurations.put(userKey, value);
      }
    }
  }

  /** Create a new configuration object with default values. */
  public StatefulFunctionsConfig() {
    this(new Configuration());
  }

  /** Returns the factory type used to serialize messages. */
  public MessageFactoryType getFactoryType() {
    return factoryType;
  }

  /** Sets the factory type used to serialize messages. */
  public void setFactoryType(MessageFactoryType factoryType) {
    this.factoryType = Objects.requireNonNull(factoryType);
  }

  /** Returns the Flink job name that appears in the Web UI. */
  public String getFlinkJobName() {
    return flinkJobName;
  }

  /** Set the Flink job name that appears in the Web UI. */
  public void setFlinkJobName(String flinkJobName) {
    this.flinkJobName = Objects.requireNonNull(flinkJobName);
  }

  /** Returns the number of bytes to use for in memory buffering of the feedback channel. */
  public MemorySize getFeedbackBufferSize() {
    return feedbackBufferSize;
  }

  /** Sets the number of bytes to use for in memory buffering of the feedback channel. */
  public void setFeedbackBufferSize(MemorySize size) {
    this.feedbackBufferSize = Objects.requireNonNull(size);
  }

  /** Returns the max async operations allowed per task. */
  public int getMaxAsyncOperationsPerTask() {
    return maxAsyncOperationsPerTask;
  }

  /** Sets the max async operations allowed per task. */
  public void setMaxAsyncOperationsPerTask(int maxAsyncOperationsPerTask) {
    this.maxAsyncOperationsPerTask = maxAsyncOperationsPerTask;
  }

  /**
   * Retrieves the universe provider for loading modules.
   *
   * @param cl The classloader on which the provider class is located.
   * @return A {@link StatefulFunctionsUniverseProvider}.
   */
  public StatefulFunctionsUniverseProvider getProvider(ClassLoader cl) {
    try {
      return InstantiationUtil.deserializeObject(universeInitializerClassBytes, cl, false);
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalStateException("Unable to initialize.", e);
    }
  }

  /** Sets the universe provider used to load modules. */
  public void setProvider(StatefulFunctionsUniverseProvider provider) {
    try {
      universeInitializerClassBytes = InstantiationUtil.serializeObject(provider);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Returns the global configurations passed to {@link
   * org.apache.flink.statefun.sdk.spi.StatefulFunctionModule#configure(Map,
   * StatefulFunctionModule.Binder)}.
   */
  public Map<String, String> getGlobalConfigurations() {
    return Collections.unmodifiableMap(globalConfigurations);
  }

  /** Adds all entries in this to the global configuration. */
  public void addAllGlobalConfigurations(Map<String, String> globalConfigurations) {
    this.globalConfigurations.putAll(globalConfigurations);
  }

  /**
   * Adds the given key/value pair to the global configuration.
   *
   * @param key the key of the key/value pair to be added
   * @param value the value of the key/value pair to be added
   */
  public void setGlobalConfiguration(String key, String value) {
    this.globalConfigurations.put(key, value);
  }

  private static void validateStrictlyRequiredConfigs(Configuration configuration) {
    final Set<String> parentFirstClassloaderPatterns =
        parentFirstClassloaderPatterns(configuration);
    if (!parentFirstClassloaderPatterns.contains("org.apache.flink.statefun")
        && !parentFirstClassloaderPatterns.contains("org.apache.kafka")
        && !parentFirstClassloaderPatterns.contains("com.google.protobuf")) {
      throw new StatefulFunctionsInvalidConfigException(
          CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
          "Must contain org.apache.flink.statefun, org.apache.kafka, and com.google.protobuf");
    }

    final int maxConcurrentCheckpoints =
        configuration.get(ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS);
    if (maxConcurrentCheckpoints != 1) {
      throw new StatefulFunctionsInvalidConfigException(
          ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS,
          "Value must be 1, since Stateful Functions currently does not support concurrent checkpoints.");
    }
  }

  private static Set<String> parentFirstClassloaderPatterns(Configuration configuration) {
    final String[] split =
        configuration.get(CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL).split(";");
    final Set<String> parentFirstClassloaderPatterns = new HashSet<>(split.length);
    for (String s : split) {
      parentFirstClassloaderPatterns.add(s.trim().toLowerCase(Locale.ENGLISH));
    }
    return parentFirstClassloaderPatterns;
  }
}
