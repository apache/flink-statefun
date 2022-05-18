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

import java.time.Duration;
import java.util.Arrays;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.statefun.e2e.smoke.SimpleVerificationServer;
import org.apache.flink.statefun.e2e.smoke.SmokeRunnerParameters;
import org.apache.flink.statefun.flink.harness.Harness;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
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
        CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
        Arrays.asList("org.apache.flink.statefun", "org.apache.kafka", "com.google.protobuf"));
    harness.withConfiguration(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    harness.withConfiguration(
        RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2147483647);
    harness.withConfiguration(
        RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(1));
    harness.withConfiguration(
        ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(2));
    harness.withConfiguration(
        ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
    harness.withConfiguration(ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, 3);
    harness.withConfiguration(CoreOptions.DEFAULT_PARALLELISM, 2);
    harness.withConfiguration(
        CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///tmp/checkpoints");

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
    parameters.setDelayCancellationOpSupported(true);
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
