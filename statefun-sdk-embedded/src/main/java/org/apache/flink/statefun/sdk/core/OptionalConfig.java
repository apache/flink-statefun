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
package org.apache.flink.statefun.sdk.core;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;

/**
 * Utility class to represent an optional config, which may have a predefined default value.
 *
 * @param <T> type of the configuration value.
 */
@ForRuntime
public final class OptionalConfig<T> {

  private final T defaultValue;
  private T value;

  public static <T> OptionalConfig<T> withDefault(T defaultValue) {
    Objects.requireNonNull(defaultValue);
    return new OptionalConfig<>(defaultValue);
  }

  public static <T> OptionalConfig<T> withoutDefault() {
    return new OptionalConfig<>(null);
  }

  private OptionalConfig(@Nullable T defaultValue) {
    this.defaultValue = defaultValue;
  }

  public void set(T value) {
    this.value = Objects.requireNonNull(value);
  }

  public T get() {
    if (!isSet() && !hasDefault()) {
      throw new NoSuchElementException(
          "A value has not been set, and no default value was defined.");
    }
    return isSet() ? value : defaultValue;
  }

  public void overwritePropertiesIfPresent(Properties properties, String key) {
    if (isSet() || (!properties.containsKey(key) && hasDefault())) {
      properties.setProperty(key, get().toString());
    }
  }

  private boolean hasDefault() {
    return defaultValue != null;
  }

  private boolean isSet() {
    return value != null;
  }
}
