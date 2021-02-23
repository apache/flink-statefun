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
package org.apache.flink.statefun.sdk.java;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public final class StatefulFunctionSpec {
  private final TypeName typeName;
  private final Map<String, ValueSpec<?>> knownValues;
  private final Supplier<? extends StatefulFunction> supplier;

  public static Builder builder(TypeName typeName) {
    return new Builder(typeName);
  }

  private StatefulFunctionSpec(
      TypeName typeName,
      Map<String, ValueSpec<?>> knownValues,
      Supplier<? extends StatefulFunction> supplier) {
    this.typeName = Objects.requireNonNull(typeName);
    this.supplier = Objects.requireNonNull(supplier);
    this.knownValues = Objects.requireNonNull(knownValues);
  }

  public TypeName typeName() {
    return typeName;
  }

  public Map<String, ValueSpec<?>> knownValues() {
    return knownValues;
  }

  public Supplier<? extends StatefulFunction> supplier() {
    return supplier;
  }

  public static final class Builder {
    private final TypeName typeName;
    private final Map<String, ValueSpec<?>> knownValues = new HashMap<>();
    private Supplier<? extends StatefulFunction> supplier;

    private Builder(TypeName typeName) {
      this.typeName = Objects.requireNonNull(typeName);
    }

    public Builder withValueSpec(ValueSpec<?> valueSpec) {
      knownValues.put(valueSpec.name(), valueSpec);
      return this;
    }

    public Builder withSupplier(Supplier<? extends StatefulFunction> supplier) {
      this.supplier = Objects.requireNonNull(supplier);
      return this;
    }

    public StatefulFunctionSpec build() {
      return new StatefulFunctionSpec(typeName, knownValues, supplier);
    }
  }
}
