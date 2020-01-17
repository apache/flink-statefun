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

package com.ververica.statefun.state.processor.example;

import com.ververica.statefun.sdk.Address;
import com.ververica.statefun.sdk.FunctionType;
import com.ververica.statefun.sdk.annotations.Persisted;
import com.ververica.statefun.sdk.io.Router;
import com.ververica.statefun.sdk.state.PersistedValue;
import com.ververica.statefun.state.processor.Context;
import com.ververica.statefun.state.processor.StateBootstrapFunction;
import com.ververica.statefun.state.processor.StatefulFunctionsSavepointCreator;
import java.util.Arrays;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * An example that demonstrates how to generate a savepoint to bootstrap function state for the
 * Greeter example. The savepoint generated with example may be used to restore the Greeter Stateful
 * Functions example.
 *
 * <p>Usage: --savepointPath [output path for generated savepoint]
 *
 * @see StatefulFunctionsSavepointCreator
 * @see StateBootstrapFunction
 */
public class GreetStatefulFunctionBootstrapExample {

  private static final FunctionType GREETER_FUNCTION_TYPE =
      new FunctionType("ververica", "greeter");

  public static void main(String[] args) throws Exception {
    final ParameterTool params = ParameterTool.fromArgs(args);
    final String savepointPath = params.getRequired("savepointPath");

    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    final DataSet<Tuple2<String, Integer>> userSeenCounts =
        env.fromCollection(
            Arrays.asList(Tuple2.of("foo", 4), Tuple2.of("bar", 3), Tuple2.of("joe", 2)));

    final StatefulFunctionsSavepointCreator newSavepoint =
        new StatefulFunctionsSavepointCreator(128);
    newSavepoint.withBootstrapData(userSeenCounts, GreetingsStateBootstrapDataRouter::new);
    newSavepoint.withStateBootstrapFunctionProvider(
        GREETER_FUNCTION_TYPE, ignored -> new GreetingsStateBootstrapFunction());
    newSavepoint.write(savepointPath);

    env.execute();
  }

  public static class GreetingsStateBootstrapDataRouter implements Router<Tuple2<String, Integer>> {
    @Override
    public void route(
        Tuple2<String, Integer> message, Downstream<Tuple2<String, Integer>> downstream) {
      downstream.forward(new Address(GREETER_FUNCTION_TYPE, message.f0), message);
    }
  }

  public static class GreetingsStateBootstrapFunction implements StateBootstrapFunction {

    @Persisted
    private final PersistedValue<Integer> seenCount =
        PersistedValue.of("seen-count", Integer.class);

    @Override
    public void bootstrap(Context context, Object bootstrapData) {
      seenCount.set(getSeenCount(bootstrapData));
    }

    @SuppressWarnings("unchecked")
    private static int getSeenCount(Object bootstrapData) {
      return ((Tuple2<String, Integer>) bootstrapData).f1;
    }
  }
}
