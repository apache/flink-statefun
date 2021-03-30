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

/** Specification for a {@link StatefulFunction}, identifiable by a unique {@link TypeName}. */
public final class StatefulFunctionSpec {
  private final TypeName typeName;
  private final Map<String, ValueSpec<?>> knownValues;
  private final Supplier<? extends StatefulFunction> supplier;

  /**
   * Creates a {@link Builder} for the spec.
   *
   * @param typeName the associated {@link TypeName} for the spec.
   */
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

  /** @return The {@link TypeName} of the function. */
  public TypeName typeName() {
    return typeName;
  }

  /** @return The registered {@link ValueSpec}s for the function. */
  public Map<String, ValueSpec<?>> knownValues() {
    return knownValues;
  }

  /** @return The supplier for instances of the function. */
  public Supplier<? extends StatefulFunction> supplier() {
    return supplier;
  }

  /** Builder for a {@link StatefulFunctionSpec}. */
  public static final class Builder {
    private final TypeName typeName;
    private final Map<String, ValueSpec<?>> knownValues = new HashMap<>();
    private Supplier<? extends StatefulFunction> supplier;

    /**
     * Creates a {@link Builder} for a {@link StatefulFunctionSpec} which is identifiable via the
     * specified {@link TypeName}.
     *
     * @param typeName the associated {@link TypeName} of the {@link StatefulFunctionSpec} being
     *     built.
     */
    private Builder(TypeName typeName) {
      this.typeName = Objects.requireNonNull(typeName);
    }

    /**
     * Registers a {@link ValueSpec} that will be used by this function. A function may only access
     * values which have a registered {@link ValueSpec}.
     *
     * @param valueSpec the value spec to register.
     * @throws IllegalArgumentException if multiple {@link ValueSpec}s with the same name was
     *     registered.
     */
    public Builder withValueSpec(ValueSpec<?> valueSpec) {
      Objects.requireNonNull(valueSpec);
      if (knownValues.put(valueSpec.name(), valueSpec) != null) {
        throw new IllegalArgumentException(
            "Attempted to register more than one ValueSpec for the state name: "
                + valueSpec.name());
      }
      return this;
    }

    /**
     * Registers multiple {@link ValueSpec}s that will be used by this function. A function may only
     * access values which have a registered {@link ValueSpec}.
     *
     * @param valueSpecs the value specs to register.
     * @throws IllegalArgumentException if multiple {@link ValueSpec}s with the same name was
     *     registered.
     */
    public Builder withValueSpecs(ValueSpec<?>... valueSpecs) {
      for (ValueSpec<?> spec : valueSpecs) {
        withValueSpec(spec);
      }
      return this;
    }

    /**
     * A {@link Supplier} used to create instances of a {@link StatefulFunction} for this {@link
     * StatefulFunctionSpec}.
     *
     * @param supplier the supplier.
     */
    public Builder withSupplier(Supplier<? extends StatefulFunction> supplier) {
      this.supplier = Objects.requireNonNull(supplier);
      return this;
    }

    /** Builds the {@link StatefulFunctionSpec}. */
    public StatefulFunctionSpec build() {
      return new StatefulFunctionSpec(typeName, knownValues, supplier);
    }
  }
}
