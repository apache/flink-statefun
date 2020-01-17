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

package com.ververica.statefun.flink.core.state;

import com.ververica.statefun.flink.core.di.Inject;
import com.ververica.statefun.flink.core.di.Label;
import com.ververica.statefun.sdk.FunctionType;
import com.ververica.statefun.sdk.state.Accessor;
import com.ververica.statefun.sdk.state.ApiExtension;
import com.ververica.statefun.sdk.state.PersistedValue;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

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
