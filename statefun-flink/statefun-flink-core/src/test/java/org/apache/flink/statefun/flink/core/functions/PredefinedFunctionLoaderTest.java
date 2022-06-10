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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.statefun.sdk.*;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.junit.Test;

public class PredefinedFunctionLoaderTest {

  private static final FunctionType TEST_TYPE = new FunctionType("namespace", "name");

  @Test
  public void exampleUsage() {
    PredefinedFunctionLoader loader =
        new PredefinedFunctionLoader(specificFunctionProviders(), Collections.emptyMap());

    StatefulFunction function = loader.load(TEST_TYPE);

    assertThat(function, notNullValue());
  }

  @Test
  public void withOnlyPerNamespaceFunctionProviders() {
    PredefinedFunctionLoader loader =
        new PredefinedFunctionLoader(Collections.emptyMap(), perNamespaceFunctionProviders());

    StatefulFunction function = loader.load(TEST_TYPE);

    assertThat(function, notNullValue());
  }

  @Test
  public void specificFunctionProvidersHigherPrecedence() {
    PredefinedFunctionLoader loader =
        new PredefinedFunctionLoader(specificFunctionProviders(), perNamespaceFunctionProviders());

    StatefulFunction function = loader.load(TEST_TYPE);

    assertThat(function, instanceOf(StatefulFunctionA.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullLoadedFunctions() {
    PredefinedFunctionLoader loader =
        new PredefinedFunctionLoader(specificFunctionProviders(), Collections.emptyMap());

    loader.load(new FunctionType("doesn't", "exist"));
  }

  private static Map<FunctionType, StatefulFunctionProvider> specificFunctionProviders() {
    final Map<FunctionType, StatefulFunctionProvider> providers = new HashMap<>();
    providers.put(
        new FunctionType(TEST_TYPE.namespace(), TEST_TYPE.name()), new SpecificFunctionProvider());
    return providers;
  }

  private static Map<String, StatefulFunctionProvider> perNamespaceFunctionProviders() {
    final Map<String, StatefulFunctionProvider> providers = new HashMap<>();
    providers.put(TEST_TYPE.namespace(), new PerNamespaceFunctionProvider());
    return providers;
  }

  private static class SpecificFunctionProvider implements StatefulFunctionProvider {
    @Override
    public org.apache.flink.statefun.sdk.StatefulFunction functionOfType(FunctionType type) {
      if (type.equals(TEST_TYPE)) {
        return new StatefulFunctionA();
      } else {
        return null;
      }
    }
  }

  private static class PerNamespaceFunctionProvider implements StatefulFunctionProvider {
    @Override
    public org.apache.flink.statefun.sdk.StatefulFunction functionOfType(FunctionType type) {
      if (type.equals(TEST_TYPE)) {
        return new StatefulFunctionB();
      } else {
        return null;
      }
    }
  }

  private static class StatefulFunctionA implements StatefulFunction {
    @Override
    public void invoke(Context context, Object input) {}
  }

  private static class StatefulFunctionB implements StatefulFunction {
    @Override
    public void invoke(Context context, Object input) {}
  }
}
