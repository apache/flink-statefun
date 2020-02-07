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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;

public class BoundState {

  public static Builder builder() {
    return new Builder();
  }

  private final List<PersistedValue<?>> persistedValues;
  private final List<PersistedTable<?, ?>> persistedTables;

  private BoundState(
      List<PersistedValue<?>> persistedValues, List<PersistedTable<?, ?>> persistedTables) {
    this.persistedValues = Objects.requireNonNull(persistedValues);
    this.persistedTables = Objects.requireNonNull(persistedTables);
  }

  @SuppressWarnings("unused")
  public List<PersistedValue<?>> persistedValues() {
    return persistedValues;
  }

  @SuppressWarnings("unused")
  public List<PersistedTable<?, ?>> getPersistedTables() {
    return persistedTables;
  }

  @SuppressWarnings("UnusedReturnValue")
  public static final class Builder {
    private List<PersistedValue<?>> persistedValues = new ArrayList<>();
    private List<PersistedTable<?, ?>> persistedTables = new ArrayList<>();

    private Builder() {}

    public Builder withPersistedValue(PersistedValue<?> persistedValue) {
      this.persistedValues.add(persistedValue);
      return this;
    }

    public Builder withPersistedTable(PersistedTable<?, ?> persistedTable) {
      this.persistedTables.add(persistedTable);
      return this;
    }

    public BoundState build() {
      return new BoundState(persistedValues, persistedTables);
    }
  }
}
