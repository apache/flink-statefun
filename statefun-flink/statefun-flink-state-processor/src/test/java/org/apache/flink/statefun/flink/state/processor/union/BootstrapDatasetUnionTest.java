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
package org.apache.flink.statefun.flink.state.processor.union;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.statefun.flink.state.processor.BootstrapDataRouterProvider;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.Router;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

public class BootstrapDatasetUnionTest {

  @Test
  public void correctTypeInformation() {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    final List<BootstrapDataset<?>> bootstrapDatasets = new ArrayList<>(2);
    bootstrapDatasets.add(
        createBootstrapDataset(env, Collections.singletonList(1), TestRouter::noOp));
    bootstrapDatasets.add(
        createBootstrapDataset(env, Collections.singletonList(true), TestRouter::noOp));

    final DataSet<TaggedBootstrapData> test = BootstrapDatasetUnion.apply(bootstrapDatasets);

    assertThat(
        test.getType(),
        is(unionTypeInfoOf(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.BOOLEAN_TYPE_INFO)));
  }

  @Test
  public void correctUnion() throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    final List<BootstrapDataset<?>> bootstrapDatasets = new ArrayList<>(2);
    bootstrapDatasets.add(
        createBootstrapDataset(
            env,
            Collections.singletonList(911108),
            () -> new TestRouter<>(addressOf("ns", "name", "id-0"), 2)));
    bootstrapDatasets.add(
        createBootstrapDataset(
            env,
            Arrays.asList(true, false),
            () -> new TestRouter<>(addressOf("ns", "name-2", "id-99"), 1)));

    final List<TaggedBootstrapData> test = BootstrapDatasetUnion.apply(bootstrapDatasets).collect();

    assertThat(
        test,
        Matchers.containsInAnyOrder(
            taggedBootstrapData(addressOf("ns", "name", "id-0"), 911108, 0),
            taggedBootstrapData(addressOf("ns", "name", "id-0"), 911108, 0),
            taggedBootstrapData(addressOf("ns", "name-2", "id-99"), true, 1),
            taggedBootstrapData(addressOf("ns", "name-2", "id-99"), false, 1)));
  }

  private static <T> BootstrapDataset<T> createBootstrapDataset(
      ExecutionEnvironment env,
      Collection<T> bootstrapDataset,
      BootstrapDataRouterProvider<T> routerProvider) {
    return new BootstrapDataset<>(env.fromCollection(bootstrapDataset), routerProvider);
  }

  private static TaggedBootstrapDataTypeInfo unionTypeInfoOf(
      TypeInformation<?>... payloadTypeInfos) {
    return new TaggedBootstrapDataTypeInfo(Arrays.asList(payloadTypeInfos));
  }

  private static Address addressOf(
      String functionNamespace, String functionName, String functionId) {
    return new Address(new FunctionType(functionNamespace, functionName), functionId);
  }

  private static Matcher<TaggedBootstrapData> taggedBootstrapData(
      Address expectedAddress, Object expectedPayload, Integer expectedUnionIndex) {
    return new TypeSafeMatcher<TaggedBootstrapData>() {
      @Override
      protected boolean matchesSafely(TaggedBootstrapData test) {
        return expectedUnionIndex == test.getUnionIndex()
            && expectedAddress.equals(test.getTarget())
            // equality checks on payload makes sense here since
            // the payloads are only booleans or integers in this test
            && expectedPayload.equals(test.getPayload());
      }

      @Override
      public void describeTo(Description description) {}
    };
  }

  private static class TestRouter<T> implements Router<T> {

    /** Number of times to route the inputs. */
    private final int routeCount;

    /** Address to route the inputs to. */
    private final Address targetAddress;

    static <T> TestRouter<T> noOp() {
      return new TestRouter<>(null, 0);
    }

    TestRouter(Address targetAddress, int routeCount) {
      this.targetAddress = targetAddress;
      this.routeCount = routeCount;
    }

    @Override
    public void route(T message, Downstream<T> downstream) {
      for (int i = 0; i < routeCount; i++) {
        downstream.forward(targetAddress, message);
      }
    }
  }
}
