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
import org.apache.flink.statefun.e2e.smoke.SimpleProtobufServer.StartedServer;
import org.apache.flink.statefun.e2e.smoke.generated.VerificationResult;
import org.apache.flink.util.function.ThrowingRunnable;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;

public class SmokeVerificationE2E {

  private static final Logger LOG = LoggerFactory.getLogger(SmokeVerificationE2E.class);

  @Test(timeout = 60_000L)
  public void runWith() throws Throwable {
    StartedServer<VerificationResult> server = startProtobufServer();

    ModuleParameters parameters = new ModuleParameters();
    parameters.setVerificationServerHost("host.testcontainers.internal");
    parameters.setVerificationServerPort(server.port());
    parameters.setNumberOfFunctionInstances(128);
    parameters.setMessageCount(100_000);
    parameters.setMaxFailures(1);

    StatefulFunctionsAppContainers.Builder builder =
        StatefulFunctionsAppContainers.builder("smoke", 2);
    builder.exposeMasterLogs(LOG);

    // set the test module parameters as global configurations, so that
    // it can be deserialized at Module#configure()
    parameters.asMap().forEach(builder::withModuleGlobalConfiguration);

    // run the test
    Testcontainers.exposeHostPorts(server.port());
    StatefulFunctionsAppContainers app = builder.build();

    run(
        app,
        () -> {
          Supplier<VerificationResult> results = server.results();
          Set<Integer> successfullyVerified = new HashSet<>();
          while (successfullyVerified.size() != parameters.getNumberOfFunctionInstances()) {
            VerificationResult result = results.get();
            if (result.getActual() == result.getExpected()) {
              successfullyVerified.add(result.getId());
            }
          }
        });
  }

  private static StartedServer<VerificationResult> startProtobufServer() {
    SimpleProtobufServer<VerificationResult> server =
        new SimpleProtobufServer<>(VerificationResult.parser());
    return server.start();
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
}
