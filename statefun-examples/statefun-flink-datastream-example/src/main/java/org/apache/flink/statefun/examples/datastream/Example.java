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

package org.apache.flink.statefun.examples.datastream;

import static org.apache.flink.statefun.flink.datastream.RequestReplyFunctionBuilder.requestReplyFunctionBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.message.RoutableMessageBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionDataStreamBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionEgressStreams;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Example {

  private static final FunctionType GREET = new FunctionType("example", "greet");
  private static final FunctionType GREET2 = new FunctionType("example", "greet2");
  private static final FunctionType REMOTE_GREET = new FunctionType("example", "remote-greet");
  private static final FunctionType REMOTE_GREET2 = new FunctionType("example", "remote-greet2");
  private static final EgressIdentifier<String> GREETINGS =
          new EgressIdentifier<>("example", "out", String.class);
  private static final EgressIdentifier<String> GREETINGS2 =
          new EgressIdentifier<>("example", "out2", String.class);

  public static void main(String... args) throws Exception {

    // -----------------------------------------------------------------------------------------
    // obtain the stream execution env and create some data streams
    // -----------------------------------------------------------------------------------------

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    Configuration conf = new Configuration();
//    conf.setString(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY, "/tmp");
//    conf.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "/tmp");
//    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
    //conf.setInteger(RestOptions.PORT, 8050);

//    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//
//    env.getConfig().enableSysoutLogging();
//    env.setParallelism(1);
    StatefulFunctionsConfig statefunConfig = StatefulFunctionsConfig.fromEnvironment(env);
    statefunConfig.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);

    DataStream<RoutableMessage> names =
        env.addSource(new NameSource())
            .map(
                name ->
                    RoutableMessageBuilder.builder()
                        .withTargetAddress(GREET, name)
                        .withMessageBody(name)
                        .build());// .uid("source step");


    // -----------------------------------------------------------------------------------------
    // wire up stateful functions
    // -----------------------------------------------------------------------------------------

    StatefulFunctionEgressStreams out =
        StatefulFunctionDataStreamBuilder.builder("example")
            .withDataStreamAsIngress(names)
            .withFunctionProvider(GREET, unused -> new MyFunction())
            .withRequestReplyRemoteFunction(
                requestReplyFunctionBuilder(
                        REMOTE_GREET, URI.create("http://localhost:5000/statefun"))
                    .withMaxRequestDuration(Duration.ofSeconds(15))
                    .withMaxNumBatchRequests(500)
            )
//            .withEgressId(GREETINGS)
            .withFunctionProvider(GREET2, unused -> new MyFunction2())
            .withRequestReplyRemoteFunction(
                    requestReplyFunctionBuilder(
                            REMOTE_GREET2, URI.create("http://localhost:5000/statefun"))
                            .withMaxRequestDuration(Duration.ofSeconds(15))
                            .withMaxNumBatchRequests(500)
            )
            .withEgressId(GREETINGS2)
            .withConfiguration(statefunConfig)
            .build(env);


    // -----------------------------------------------------------------------------------------
    // obtain the outputs
    // -----------------------------------------------------------------------------------------

    DataStream<String> output2 = out.getDataStreamForEgressId(GREETINGS2);

    // -----------------------------------------------------------------------------------------
    // the rest of the pipeline
    // -----------------------------------------------------------------------------------------

    output2
        .map(
            new RichMapFunction<String, String>() {
              @Override
              public String map(String value) {
                System.out.println(value);
                return "'" + value + "'";
              }
            })
        .addSink(new PrintSinkFunction<>());

    System.out.println("Plan 4 " + env.getExecutionPlan());
    //System.out.print(env.getStreamGraph("Flink Streaming Job", false));
    env.execute();
  }

  private static final class MyFunction implements StatefulFunction {

    @Persisted
    private final PersistedValue<Integer> seenCount = PersistedValue.of("seen", Integer.class);

    @Override
    public void invoke(Context context, Object input) {
      int seen = seenCount.updateAndGet(MyFunction::increment);
      System.out.println("MyFunction: " + input.toString());
      context.send(GREET2, input.toString(), (String)input);
    }

    private static int increment(@Nullable Integer n) {
      return n == null ? 1 : n + 1;
    }
  }

  private static final class MyFunction2 implements StatefulFunction {

    @Persisted
    private final PersistedValue<Integer> seenCount2 = PersistedValue.of("seen", Integer.class);

    @Override
    public void invoke(Context context, Object input) {
      int seen = seenCount2.updateAndGet(MyFunction2::increment);
      System.out.println("MyFunction2: " + input.toString());
      context.send(GREETINGS2, String.format("seen2: Hello %s at the %d-th time", input, seen));
    }

    private static int increment(@Nullable Integer n) {
      return n == null ? 1 : n + 1;
    }
  }

  private static final class NameSource implements SourceFunction<String> {

    private static final long serialVersionUID = 1;

    private volatile boolean canceled;

    @Override
    public void run(SourceContext<String> ctx) throws InterruptedException {
      String[] names = {"Stephan", "Igal", "Gordon", "Seth", "Marta"};
      ThreadLocalRandom random = ThreadLocalRandom.current();
      while (true) {
        int index = random.nextInt(names.length);
        final String name = names[index];
        synchronized (ctx.getCheckpointLock()) {
          if (canceled) {
            return;
          }
          ctx.collect(name);
        }
        Thread.sleep(1000);
      }
    }

    @Override
    public void cancel() {
      canceled = true;
    }
  }
}
