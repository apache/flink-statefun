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

package org.apache.flink.statefun.flink.datastream;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.types.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamCollector;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/** IT Case for Stateful Function interop. */
public class StatefulFunctionsDataStreamInteropITCase {

  private static final FunctionType TYPE = new FunctionType("example", "test");

  private static final GenericEgress<String> greetings =
      GenericEgress.named(TypeName.parseFrom("example/egress")).withUtf8StringType();

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberSlotsPerTaskManager(2)
              .setNumberTaskManagers(1)
              .build());

  @Rule public StreamCollector collector = new StreamCollector();

  @Test
  public void testDataStreamInterop() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();

    DataStream<String> stream = env.fromElements("John", "Sally");

    StatefulFunctionDataStreamBuilder builder =
        StatefulFunctionDataStreamBuilder.builder("datastream-interop")
            .withDataStreamIngress(stream, name -> new Address(TYPE, name))
            .withEndpoint(Endpoint.withSpec("remote/specific", "http://endpoint"))
            .withEndpoint(Endpoint.withSpec("remote/*", "http://endpoint/{function.name}"))
            .withFunctionProvider(
                TYPE, (SerializableStatefulFunctionProvider) type -> new TestFunction())
            .withGenericEgress(greetings);

    StatefulFunctionEgressStreams egressStreams = builder.build(env);

    DataStream<String> egress = egressStreams.getDataStream(greetings);
    CompletableFuture<Collection<String>> results = collector.collect(egress);

    env.execute();

    Assert.assertThat(
        "unexpected output from Stateful Functions environment",
        results.get(),
        Matchers.hasItems("John", "Sally"));
  }

  public static class TestFunction implements StatefulFunction {

    @Override
    public void invoke(Context context, Object input) {
      Assert.assertThat("unexpected input data type", input, Matchers.instanceOf(TypedValue.class));
      TypedValue typedValue = (TypedValue) input;
      String message =
          Types.stringType()
              .typeSerializer()
              .deserialize(SliceProtobufUtil.asSlice(typedValue.getValue()));
      Assert.assertEquals("unexpected address id for record", context.self().id(), message);
      context.send(StatefulFunctionsDataStreamInteropITCase.greetings.getId(), typedValue);
    }
  }
}
