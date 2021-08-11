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

package org.apache.flink.statefun.e2e.smoke;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.flink.statefun.e2e.common.StatefulFunctionsAppContainers;
import org.apache.flink.statefun.e2e.smoke.generated.VerificationResult;
import org.apache.flink.util.function.ThrowingRunnable;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;

public final class SmokeRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SmokeRunner.class);

  public static void run(
      SmokeRunnerParameters parameters, StatefulFunctionsAppContainers.Builder builder)
      throws Throwable {
    // start verification server
    SimpleVerificationServer.StartedServer server = new SimpleVerificationServer().start();
    parameters.setVerificationServerHost("host.testcontainers.internal");
    parameters.setVerificationServerPort(server.port());
    Testcontainers.exposeHostPorts(server.port());

    // set the test module parameters as global configurations, so that
    // it can be deserialized at Module#configure()
    parameters.asMap().forEach(builder::withModuleGlobalConfiguration);
    builder.exposeMasterLogs(LOG);
    StatefulFunctionsAppContainers app = builder.build();

    // run the test
    run(
        app,
        () ->
            awaitVerificationSuccess(server.results(), parameters.getNumberOfFunctionInstances()));
  }

  private static void run(StatefulFunctionsAppContainers app, ThrowingRunnable<Throwable> r)
      throws Throwable {
    Statement statement =
        app.apply(
            new Statement() {
              @Override
              public void evaluate() throws Throwable {
                r.run();
              }
            },
            Description.EMPTY);

    statement.evaluate();
  }

  public static void awaitVerificationSuccess(
      Supplier<VerificationResult> results, final int numberOfFunctionInstances) {
    Set<Integer> successfullyVerified = new HashSet<>();
    while (successfullyVerified.size() != numberOfFunctionInstances) {
      VerificationResult result = results.get();
      if (result.getActual() == result.getExpected()) {
        successfullyVerified.add(result.getId());
      } else if (result.getActual() > result.getExpected()) {
        throw new AssertionError(
            "Over counted. Expected: "
                + result.getExpected()
                + ", actual: "
                + result.getActual()
                + ", function: "
                + result.getId());
      }
    }
  }
}
