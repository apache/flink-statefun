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
package org.apache.flink.statefun.flink.core.common;

import java.io.IOException;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.InstantiationUtil;

public final class ConfigurationUtil {
  private ConfigurationUtil() {}

  public static <T> T getSerializedInstance(
      ClassLoader classLoader, Configuration configuration, ConfigOption<byte[]> option) {
    final byte[] bytes = configuration.getBytes(option.key(), option.defaultValue());
    try {
      return InstantiationUtil.deserializeObject(bytes, classLoader, false);
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalStateException("Unable to initialize.", e);
    }
  }

  public static void storeSerializedInstance(
      Configuration configuration, ConfigOption<byte[]> option, Object instance) {
    try {
      byte[] bytes = InstantiationUtil.serializeObject(instance);
      configuration.setBytes(option.key(), bytes);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
