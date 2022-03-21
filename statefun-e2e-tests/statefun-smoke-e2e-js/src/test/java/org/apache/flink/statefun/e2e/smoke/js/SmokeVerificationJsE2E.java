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

package org.apache.flink.statefun.e2e.smoke.js;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.flink.statefun.e2e.common.StatefulFunctionsAppContainers;
import org.apache.flink.statefun.e2e.smoke.SmokeRunner;
import org.apache.flink.statefun.e2e.smoke.SmokeRunnerParameters;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;

public class SmokeVerificationJsE2E {

  private static final Logger LOG = LoggerFactory.getLogger(SmokeVerificationJsE2E.class);
  private static final int NUM_WORKERS = 2;

  @Test(timeout = 1_000 * 60 * 10)
  public void runWith() throws Throwable {
    SmokeRunnerParameters parameters = new SmokeRunnerParameters();
    parameters.setNumberOfFunctionInstances(128);
    parameters.setMessageCount(100_000);
    parameters.setMaxFailures(1);

    GenericContainer<?> remoteFunction = configureRemoteFunction();

    StatefulFunctionsAppContainers.Builder builder =
        StatefulFunctionsAppContainers.builder("flink-statefun-cluster", NUM_WORKERS)
            .withBuildContextFileFromClasspath("remote-module", "/remote-module/")
            .withModuleGlobalConfiguration("REMOTE_FUNCTION_HOST", "remote-function-host")
            .dependsOn(remoteFunction);

    SmokeRunner.run(parameters, builder);
  }

  private GenericContainer<?> configureRemoteFunction() {
    ImageFromDockerfile remoteFunctionImage =
        new ImageFromDockerfile("remote-function-image")
            .withFileFromClasspath("Dockerfile", "Dockerfile.remote-function")
            .withFileFromPath("sdk", sdkPath())
            .withFileFromClasspath("remote-function/", "remote-function/");

    return new GenericContainer<>(remoteFunctionImage)
        .withNetworkAliases("remote-function-host")
        .withLogConsumer(new Slf4jLogConsumer(LOG));
  }

  private static Path sdkPath() {
    return Paths.get(System.getProperty("user.dir") + "/../../statefun-sdk-js");
  }
}
