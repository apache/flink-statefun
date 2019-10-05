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

package com.ververica.statefun.flink.io.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.flink.annotation.Internal;

@Internal
public final class ReflectionUtil {

  private ReflectionUtil() {}

  public static <T> T instantiate(Class<T> type) {
    try {
      Constructor<T> defaultConstructor = type.getDeclaredConstructor();
      defaultConstructor.setAccessible(true);
      return defaultConstructor.newInstance();
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          "Unable to create an instance of " + type.getName() + " has no default constructor", e);
    } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
      throw new IllegalStateException("Unable to create an instance of " + type.getName(), e);
    }
  }
}
