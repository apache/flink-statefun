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

package org.apache.flink.statefun.itcases.sanity.testutils;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * A JUnit {@link org.junit.rules.TestRule} that setups a containerized Stateful Functions
 * application using <a href="https://www.testcontainers.org/">Testcontainers</a>. This allows
 * composing end-to-end tests for Stateful Functions applications easier, by managing the
 * containerized application as an external test resource whose lifecycle is integrated with the
 * JUnit test framework.
 *
 * <h2>Example usage</h2>
 *
 * <pre>{@code
 * public class MyITCase {
 *
 *     {@code @Rule}
 *     public StatefulFunctionsAppContainers myApp =
 *         new StatefulFunctionsAppContainers("app-name", 3);
 *
 *     {@code @Test}
 *     public void runTest() {
 *         // the containers for the app, including master and workers, will already be running
 *         // before the test is run; implement your test logic against the app
 *     }
 * }
 * }</pre>
 *
 * <p>In most cases you'd also need to start an additional system for the test, for example starting
 * a container that runs Kafka from which the application depends on as an ingress or egress. The
 * following demonstrates adding a Kafka container to the setup:
 *
 * <pre>{@code
 * public class MyKafkaITCase {
 *
 *     {@code @Rule}
 *     public KafkaContainer kafka = new KafkaContainer();
 *
 *     {@code @Rule}
 *     public StatefulFunctionsAppContainers myApp =
 *         new StatefulFunctionsAppContainers("app-name", 3)
 *             .dependsOn(kafka);
 *
 *     ...
 * }
 * }</pre>
 *
 * <p>Application master and worker containers will always be started after containers that are
 * added using {@link #dependsOn(GenericContainer)} have started. Moreover, containers being
 * depended on will also be setup such that they share the same network with the master and workers,
 * so that they can freely communicate with each other.
 *
 * <h2>Prerequisites</h2>
 *
 * <p>Since Testcontainers uses Docker, it is required that you have Docker installed for this test
 * rule to work.
 *
 * <p>When building the Docker image for the Stateful Functions application under test, the
 * following files are added to the build context:
 *
 * <uL>
 *   <li>The {@code Dockerfile} found at path {@code /Dockerfile} in the classpath. This is required
 *       to be present. A simple way is to add the Dockerfile to the test resources directory. This
 *       will be added to the root of the Docker image build context.
 *   <li>The {@code flink-conf.yaml} found at path {@code /flink-conf.yaml} in the classpath, if
 *       any. You can also add this to the test resources directory. This will be added to the root
 *       of the Docker image build context.
 *   <li>All built artifacts under the generated {@code target} folder for the project module that
 *       the test resides in. This is required to be present, so this entails that the tests can
 *       only be ran after artifacts are built. The built artifacts are added to the root of the
 *       Docker image build context.
 * </uL>
 */
public final class StatefulFunctionsAppContainers extends ExternalResource {

  private static final Logger LOG = LoggerFactory.getLogger(StatefulFunctionsAppContainers.class);

  private static final String MASTER_HOST = "statefun-app-master";
  private static final String WORKER_HOST_PREFIX = "statefun-app-worker";

  private final Network network;
  private final GenericContainer master;
  private final List<GenericContainer> workers;

  public StatefulFunctionsAppContainers(String appName, int numWorkers) {
    this(appName, numWorkers, null);
  }

  public StatefulFunctionsAppContainers(
      String appName, int numWorkers, @Nullable Configuration dynamicProperties) {
    if (appName == null || appName.isEmpty()) {
      throw new IllegalArgumentException(
          "App name must be non-empty. This is used as the application image name.");
    }
    if (numWorkers < 1) {
      throw new IllegalArgumentException("Must have at least 1 worker.");
    }

    this.network = Network.newNetwork();

    final ImageFromDockerfile appImage = appImage(appName, dynamicProperties);
    this.master = masterContainer(appImage, network);
    this.workers = workerContainers(appImage, numWorkers, network);
  }

  public StatefulFunctionsAppContainers dependsOn(GenericContainer container) {
    container.withNetwork(network);
    master.dependsOn(container);
    return this;
  }

  public StatefulFunctionsAppContainers exposeMasterLogs(Logger logger) {
    master.withLogConsumer(new Slf4jLogConsumer(logger, true));
    return this;
  }

  @Override
  protected void before() throws Throwable {
    master.start();
    workers.forEach(GenericContainer::start);
  }

  @Override
  protected void after() {
    master.stop();
    workers.forEach(GenericContainer::stop);
  }

  private static ImageFromDockerfile appImage(
      String appName, @Nullable Configuration dynamicProperties) {
    final Path targetDirPath = Paths.get(System.getProperty("user.dir") + "/target/");
    LOG.info("Building app image with built artifacts located at: {}", targetDirPath);

    final ImageFromDockerfile appImage =
        new ImageFromDockerfile(appName)
            .withFileFromClasspath("Dockerfile", "Dockerfile")
            .withFileFromPath(".", targetDirPath);

    Configuration flinkConf = loadFlinkConfIfAvailable(dynamicProperties);
    if (flinkConf != null) {
      appImage.withFileFromString("flink-conf.yaml", flinkConfigAsString(flinkConf));
    }

    return appImage;
  }

  private static @Nullable Configuration loadFlinkConfIfAvailable(
      @Nullable Configuration dynamicProperties) {
    final URL flinkConfUrl = StatefulFunctionsAppContainers.class.getResource("/flink-conf.yaml");
    if (flinkConfUrl == null) {
      return dynamicProperties;
    }

    final String flinkConfDir;
    try {
      flinkConfDir = new File(flinkConfUrl.toURI()).getParentFile().getAbsolutePath();
    } catch (URISyntaxException e) {
      throw new RuntimeException("Failed to load flink-conf.yaml", e);
    }

    return GlobalConfiguration.loadConfiguration(flinkConfDir, dynamicProperties);
  }

  private static String flinkConfigAsString(Configuration configuration) {
    StringBuilder yaml = new StringBuilder();
    for (Map.Entry<String, String> entry : configuration.toMap().entrySet()) {
      yaml.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
    }

    return yaml.toString();
  }

  private static GenericContainer masterContainer(ImageFromDockerfile appImage, Network network) {
    return new GenericContainer(appImage)
        .withNetwork(network)
        .withNetworkAliases(MASTER_HOST)
        .withEnv("ROLE", "master")
        .withEnv("MASTER_HOST", MASTER_HOST);
  }

  private static List<GenericContainer> workerContainers(
      ImageFromDockerfile appImage, int numWorkers, Network network) {
    final List<GenericContainer> workers = new ArrayList<>(numWorkers);

    for (int i = 0; i < numWorkers; i++) {
      workers.add(
          new GenericContainer(appImage)
              .withNetwork(network)
              .withNetworkAliases(workerHostOf(i))
              .withEnv("ROLE", "worker")
              .withEnv("MASTER_HOST", MASTER_HOST));
    }

    return workers;
  }

  private static String workerHostOf(int workerIndex) {
    return WORKER_HOST_PREFIX + "-" + workerIndex;
  }
}
