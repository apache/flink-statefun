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

package org.apache.flink.statefun.e2e.smoke.embedded;

import static org.apache.flink.statefun.e2e.smoke.SmokeRunner.awaitVerificationSuccess;

import org.apache.flink.statefun.e2e.smoke.SimpleVerificationServer;
import org.apache.flink.statefun.e2e.smoke.SmokeRunnerParameters;
import org.apache.flink.statefun.flink.harness.Harness;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedSmokeHarnessTest {

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedSmokeHarnessTest.class);

  @Ignore
  @Test(timeout = 1_000 * 60 * 2)
  public void miniClusterTest() throws Exception {
    Harness harness = new Harness();

    // set Flink related configuration.
    harness.withConfiguration(
        "classloader.parent-first-patterns.additional",
        "org.apache.flink.statefun;org.apache.kafka;com.google.protobuf");
    harness.withConfiguration("restart-strategy", "fixed-delay");
    harness.withConfiguration("restart-strategy.fixed-delay.attempts", "2147483647");
    harness.withConfiguration("restart-strategy.fixed-delay.delay", "1sec");
    harness.withConfiguration("execution.checkpointing.interval", "2sec");
    harness.withConfiguration("execution.checkpointing.mode", "EXACTLY_ONCE");
    harness.withConfiguration("execution.checkpointing.max-concurrent-checkpoints", "3");
    harness.withConfiguration("parallelism.default", "2");
    harness.withConfiguration("state.checkpoints.dir", "file:///tmp/checkpoints");

    // start the verification server
    SimpleVerificationServer.StartedServer started = new SimpleVerificationServer().start();

    // configure test parameters.
    SmokeRunnerParameters parameters = new SmokeRunnerParameters();
    parameters.setMaxFailures(1);
    parameters.setMessageCount(100_000);
    parameters.setNumberOfFunctionInstances(128);
    parameters.setVerificationServerHost("localhost");
    parameters.setVerificationServerPort(started.port());
    parameters.setAsyncOpSupported(true);
    parameters.asMap().forEach(harness::withGlobalConfiguration);

    // run the harness.
    try (AutoCloseable ignored = startHarnessInTheBackground(harness)) {
      awaitVerificationSuccess(started.results(), parameters.getNumberOfFunctionInstances());
    }

    LOG.info("All done.");
  }

  private static AutoCloseable startHarnessInTheBackground(Harness harness) {
    Thread t =
        new Thread(
            () -> {
              try {
                harness.start();
              } catch (InterruptedException ignored) {
                LOG.info("Harness Thread was interrupted. Exiting...");
              } catch (Exception exception) {
                LOG.info("Something happened while trying to run the Harness.", exception);
              }
            });
    t.setName("harness-runner");
    t.setDaemon(true);
    t.start();
    return t::interrupt;
  }
}
