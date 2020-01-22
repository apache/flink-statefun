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
package org.apache.flink.statefun.flink.core.state;

import java.io.IOException;
import java.util.Objects;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.statefun.sdk.state.Accessor;

final class FlinkValueAccessor<T> implements Accessor<T> {

  private final ValueState<T> handle;

  FlinkValueAccessor(ValueState<T> handle) {
    this.handle = Objects.requireNonNull(handle);
  }

  @Override
  public void set(T value) {
    try {
      if (value == null) {
        handle.clear();
      } else {
        handle.update(value);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public T get() {
    try {
      return handle.value();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    handle.clear();
  }
}
