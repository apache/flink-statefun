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
package org.apache.flink.statefun.flink.core.classloader;

import java.io.IOException;
import org.apache.flink.runtime.util.ClassLoaderUtil;
import org.apache.flink.util.InstantiationUtil;

public class SerializedValue<T> implements java.io.Serializable {

  public static final long serialVersionUID = 1L;

  private final byte[] serializedData;

  private SerializedValue(byte[] serializedData) {
    this.serializedData = serializedData;
  }

  public static <T> SerializedValue<T> of(T value) {
    try {
      byte[] serializedData = InstantiationUtil.serializeObject(value);
      return new SerializedValue<>(serializedData);
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize value", e);
    }
  }

  @SuppressWarnings("unchecked")
  public T deserialize(ClassLoader cl) {
    try {
      return serializedData == null
          ? null
          : (T) InstantiationUtil.deserializeObject(serializedData, cl);
    } catch (ClassNotFoundException e) {
      String classLoaderInfo = ClassLoaderUtil.getUserCodeClassLoaderInfo(cl);
      boolean loadableDoubleCheck = ClassLoaderUtil.validateClassLoadable(e, cl);

      String exceptionMessage =
          "Cannot load user class: "
              + e.getMessage()
              + "\nClassLoader info: "
              + classLoaderInfo
              + (loadableDoubleCheck
                  ? "\nClass was actually found in classloader - deserialization issue."
                  : "\nClass not resolvable through given classloader.");

      throw new RuntimeException(exceptionMessage);
    } catch (IOException e) {
      throw new RuntimeException("Cannot instantiate value.", e);
    }
  }
}
