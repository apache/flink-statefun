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
package org.apache.flink.statefun.flink.core.functions;

import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.flink.common.SetContextClassLoader;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

/** An {@link FunctionLoader} that has a predefined set of {@link StatefulFunctionProvider}s. */
final class PredefinedFunctionLoader implements FunctionLoader {
  private final Map<FunctionType, StatefulFunctionProvider> functionProviders;

  @Inject
  PredefinedFunctionLoader(
      @Label("function-providers") Map<FunctionType, StatefulFunctionProvider> functionProviders) {
    this.functionProviders = Objects.requireNonNull(functionProviders);
  }

  @Override
  public StatefulFunction load(FunctionType functionType) {
    Objects.requireNonNull(functionType);
    StatefulFunctionProvider provider = functionProviders.get(functionType);
    if (provider == null) {
      throw new IllegalArgumentException("Unknown provider for type " + functionType);
    }
    StatefulFunction statefulFunction = load(provider, functionType);
    if (statefulFunction == null) {
      throw new IllegalStateException(
          "A provider for a type " + functionType + " has produced a NULL function");
    }
    return statefulFunction;
  }

  private static StatefulFunction load(
      StatefulFunctionProvider provider, FunctionType functionType) {
    try (SetContextClassLoader ignored = new SetContextClassLoader(provider)) {
      return provider.functionOfType(functionType);
    }
  }
}
