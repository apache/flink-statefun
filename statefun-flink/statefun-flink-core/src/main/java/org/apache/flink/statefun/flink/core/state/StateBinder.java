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

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.state.Accessor;
import org.apache.flink.statefun.sdk.state.ApiExtension;
import org.apache.flink.statefun.sdk.state.PersistedValue;

public final class StateBinder {
  private final State state;

  @Inject
  public StateBinder(@Label("state") State state) {
    this.state = Objects.requireNonNull(state);
  }

  public BoundState bind(FunctionType functionType, @Nullable Object instance) {
    List<PersistedValue<Object>> values = PersistedValues.findReflectively(instance);

    for (PersistedValue<Object> persistedValue : values) {
      Accessor<Object> accessor = state.createFlinkStateAccessor(functionType, persistedValue);
      ApiExtension.setPersistedValueAccessor(persistedValue, accessor);
    }

    return new BoundState(values);
  }
}
