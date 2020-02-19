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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.flink.util.InstantiationUtil;

/** Configuration that captures all stateful function related settings. */
@SuppressWarnings("WeakerAccess")
public class StatefulFunctionsConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final String MODULE_CONFIG_PREFIX = "statefun.module.config.";

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

  private MessageFactoryType factoryType;

  private String flinkJobName;

  private byte[] universeInitializerClassBytes;

  private Map<String, String> globalConfigurations = new HashMap<>();

  /**
   * Create a new configuration object based on the values set in flink-conf.
   *
   * @param configuration a configuration to read the values from
   */
  public StatefulFunctionsConfig(Configuration configuration) {
    this.factoryType = configuration.get(USER_MESSAGE_SERIALIZER);
    this.flinkJobName = configuration.get(FLINK_JOB_NAME);

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

  /** @return factoryType The factory type used to serialize messages. */
  public MessageFactoryType getFactoryType() {
    return factoryType;
  }

  /** @param factoryType The factory type used to serialize messages. */
  public void setFactoryType(MessageFactoryType factoryType) {
    this.factoryType = Objects.requireNonNull(factoryType);
  }

  /** @return The Flink job name that appears in the Web UI. */
  public String getFlinkJobName() {
    return flinkJobName;
  }

  /** @param flinkJobName The Flink job name that appears in the Web UI. */
  public void setFlinkJobName(String flinkJobName) {
    this.flinkJobName = Objects.requireNonNull(flinkJobName);
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
   * @return The global configurations passed to {@link
   *     org.apache.flink.statefun.sdk.spi.StatefulFunctionModule#configure(Map,
   *     StatefulFunctionModule.Binder)}.
   */
  public Map<String, String> getGlobalConfigurations() {
    return Collections.unmodifiableMap(globalConfigurations);
  }

  public void setGlobalConfigurations(Map<String, String> globalConfigurations) {
    this.globalConfigurations.putAll(globalConfigurations);
  }
}
