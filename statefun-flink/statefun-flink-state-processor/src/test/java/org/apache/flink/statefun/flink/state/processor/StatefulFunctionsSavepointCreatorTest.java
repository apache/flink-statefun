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

package org.apache.flink.statefun.flink.state.processor;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.Router;
import org.junit.Test;

public class StatefulFunctionsSavepointCreatorTest {

  @Test(expected = IllegalArgumentException.class)
  public void invalidMaxParallelism() {
    new StatefulFunctionsSavepointCreator(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void duplicateStateBootstrapFunctionProvider() {
    final StatefulFunctionsSavepointCreator testCreator = new StatefulFunctionsSavepointCreator(1);

    testCreator.withStateBootstrapFunctionProvider(
        new FunctionType("ns", "test"), ignored -> new NoOpStateBootstrapFunction());
    testCreator.withStateBootstrapFunctionProvider(
        new FunctionType("ns", "test"), ignored -> new NoOpStateBootstrapFunction());
  }

  @Test(expected = IllegalStateException.class)
  public void noBootstrapDataOnWrite() {
    final StatefulFunctionsSavepointCreator testCreator = new StatefulFunctionsSavepointCreator(1);

    testCreator.withStateBootstrapFunctionProvider(
        new FunctionType("ns", "test"), ignored -> new NoOpStateBootstrapFunction());
    testCreator.write("ignored");
  }

  @Test(expected = IllegalStateException.class)
  public void noStateBootstrapFunctionProvidersOnWrite() {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    final StatefulFunctionsSavepointCreator testCreator = new StatefulFunctionsSavepointCreator(1);

    testCreator.withBootstrapData(env.fromElements("foobar"), NoOpBootstrapDataRouter::new);
    testCreator.write("ignored");
  }

  private static class NoOpStateBootstrapFunction implements StateBootstrapFunction {
    @Override
    public void bootstrap(Context context, Object bootstrapData) {}
  }

  private static class NoOpBootstrapDataRouter<T> implements Router<T> {
    @Override
    public void route(T message, Downstream<T> downstream) {}
  }
}
