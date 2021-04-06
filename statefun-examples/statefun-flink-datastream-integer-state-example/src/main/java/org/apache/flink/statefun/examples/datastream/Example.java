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
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.message.RoutableMessageBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionDataStreamBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionEgressStreams;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.state.PersistedAsyncValue;
import org.apache.flink.statefun.sdk.state.PersistedIntegerValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Example {

    private static final FunctionType GREET = new FunctionType("example", "greet");
    private static final FunctionType REMOTE_GREET = new FunctionType("example", "remote-greet");
    private static final EgressIdentifier<String> GREETINGS =
            new EgressIdentifier<>("example", "out", String.class);
    private static final EgressIdentifier<String> GREETINGS2 =
            new EgressIdentifier<>("example", "out2", String.class);
    private static final EgressIdentifier<String> GREETINGS3 =
            new EgressIdentifier<>("example", "out3", String.class);
    private static final EgressIdentifier<String> GREETINGS4 =
            new EgressIdentifier<>("example", "out4", String.class);

    public static void main(String... args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        int num = 200000;
        int keys = 5;
        if (params.has("num")) {
            // read the text file from given input path
            num = Integer.parseInt(params.get("num"));
        }
        if (params.has("keys")) {
            // read the text file from given input path
            keys = Integer.parseInt(params.get("keys"));
        }
        // -----------------------------------------------------------------------------------------
        // obtain the stream execution env and create some data streams
        // -----------------------------------------------------------------------------------------

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.setString(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY, "/tmp");
        conf.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "/tmp");
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

        env.getConfig().enableSysoutLogging();
        StatefulFunctionsConfig statefunConfig = StatefulFunctionsConfig.fromEnvironment(env);
        statefunConfig.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);

        System.out.print(env.getConfig());
        DataStream<RoutableMessage> names =
                env.addSource(new NameSource(num, keys))
                        .map(
                                name ->
                                        RoutableMessageBuilder.builder()
                                                .withTargetAddress(GREET, name)
                                                .withMessageBody(name)
                                                .build()); // .uid("source step");

        // -----------------------------------------------------------------------------------------
        // wire up stateful functions
        // -----------------------------------------------------------------------------------------

        StatefulFunctionDataStreamBuilder builder =
                StatefulFunctionDataStreamBuilder.builder("example")
                        .withDataStreamAsIngress(names)
                        .withFunctionProvider(GREET, unused -> new MyFunction())
                        .withRequestReplyRemoteFunction(
                                requestReplyFunctionBuilder(
                                        REMOTE_GREET, URI.create("http://localhost:5000/statefun"))
                                        .withMaxRequestDuration(Duration.ofSeconds(60))
                                        .withMaxNumBatchRequests(50000))
                        .withEgressId(GREETINGS);

        StatefulFunctionEgressStreams out = builder.withConfiguration(statefunConfig).build(env);

        DataStream<String> output = out.getDataStreamForEgressId(GREETINGS);

        output
                .map(
                        new RichMapFunction<String, String>() {
                            @Override
                            public String map(String value) {
                                System.out.println(value);
                                return "' output  " + value + "'";
                            }
                        })
                .addSink(new PrintSinkFunction<>());

        System.out.println("Plan 4 " + env.getExecutionPlan());
        env.execute();
    }

    private static final class MyFunction implements StatefulFunction {

        @Persisted
        private final PersistedIntegerValue seenCount = PersistedIntegerValue.of("syncIntegerSeen");

        MyFunction(){
            seenCount.set(0L);
        }

        @Override
        public void invoke(Context context, Object input) {
                Long ret = seenCount.increment();
                context.send(GREETINGS, String.format("MyFunction seen: Hello %s at the %d-th time", input, ret));
        }
    }

    private static final class MyFunction2 implements StatefulFunction {
        @Persisted
        private final PersistedAsyncValue<Integer> asyncSeenCount2 = PersistedAsyncValue.of("asyncSeen2", Integer.class);


        @Override
        public void invoke(Context context, Object input) {

            if (input instanceof AsyncOperationResult){
                AsyncOperationResult result = (AsyncOperationResult)input;
                if(result.successful()){
                    if (((metadata2)result.metadata()).inputName.contains("Name")){
                        System.out.println("MyFunction2 step 2 seen 1 " + result.metadata()  + " asyncSeenCount " + ((metadata2)(result.metadata())).asyncOrder + " thread " + Thread.currentThread().getName());
                        synchronized (context) {
                            context.send(GREETINGS, String.format("MyFunction2  seen: Hello %s at the %d-th time", ((metadata2) (result.metadata())).inputName, ((metadata2) (result.metadata())).asyncOrder));
                        }
                    }
                    else{
                        System.out.println("MyFunction2 step 1 " + result.value() + " meta " + ((metadata2)result.metadata()).inputName + " : " + ((metadata2)result.metadata()).asyncOrder + " thread " + Thread.currentThread().getName());
                        CompletableFuture<String> seenFuture2 = CompletableFuture.supplyAsync(()-> {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            return "OK";
                        });
                        synchronized (context){
                            context.registerAsyncOperation(new metadata2(((metadata2)result.metadata()).inputName + " Name",(int)result.value()), seenFuture2);
                        }
                    }
                }
            }
            else{
                CompletableFuture<Integer> seenFuture = CompletableFuture.supplyAsync(()-> {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return 0;
                });
                System.out.println("MyFunction2 step 0 ceeating future complete thread " + Thread.currentThread().getName() + " input " + input);
                synchronized (context) {
                    context.send(GREETINGS, String.format("MyFunction2  seen: Hello %s at the %d-th time", input, 0));
                }
            }
        }

        class metadata2{
            String inputName;
            int asyncOrder;

            metadata2(String name, int order){
                inputName = name;
                order = asyncOrder;
            }
        }

    }

    private static final class NameSource implements SourceFunction<String> {

        private static final long serialVersionUID = 1;

        private volatile boolean canceled;

        private int numItems;

        private int numKeys;

        public NameSource(int num, int keys){
            numItems = num;
            numKeys = keys;
        }

        @Override
        public void run(SourceContext<String> ctx) throws InterruptedException {
            ArrayList<String> names = new ArrayList<String>();
            for(int i = 0; i < numKeys; i++){
                String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
                StringBuilder salt = new StringBuilder();
                Random rnd = new Random();
                while (salt.length() < 18) { // length of the random string.
                    int index = (int) (rnd.nextFloat() * SALTCHARS.length());
                    salt.append(SALTCHARS.charAt(index));
                }
                String saltStr = salt.toString();
                names.add(saltStr);
            }

            int count = 0;
            while (true) {
                int index = count % numKeys;
                final String name = names.get(index);
                synchronized (ctx.getCheckpointLock()) {
                    if (canceled) {
                        return;
                    }
                    ctx.collect(name);
                }
                if (count++ > numItems) {
                    break;
                }
            }
        }

        @Override
        public void cancel() {
            canceled = true;
        }
    }
}
