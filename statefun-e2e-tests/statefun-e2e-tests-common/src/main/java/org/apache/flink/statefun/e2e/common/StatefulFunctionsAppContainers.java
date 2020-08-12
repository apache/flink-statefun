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

package org.apache.flink.statefun.e2e.common;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.util.FileUtils;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
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
 * public class MyE2E {
 *
 *     {@code @Rule}
 *     public StatefulFunctionsAppContainers myApp =
 *         StatefulFunctionsAppContainers.builder("app-name", 3).build();
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
 * public class MyKafkaE2E {
 *
 *     {@code @Rule}
 *     public KafkaContainer kafka = new KafkaContainer();
 *
 *     {@code @Rule}
 *     public StatefulFunctionsAppContainers myApp =
 *         StatefulFunctionsAppContainers.builder("app-name", 3)
 *             .dependsOn(kafka)
 *             .build();
 *
 *     ...
 * }
 * }</pre>
 *
 * <p>Application master and worker containers will always be started after containers that are
 * added using {@link Builder#dependsOn(GenericContainer)} have started. Moreover, containers being
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

  private GenericContainer<?> master;
  private List<GenericContainer<?>> workers;

  private File checkpointDir;

  private StatefulFunctionsAppContainers(
      GenericContainer<?> masterContainer, List<GenericContainer<?>> workerContainers) {
    this.master = Objects.requireNonNull(masterContainer);
    this.workers = Objects.requireNonNull(workerContainers);
  }

  /**
   * Creates a builder for creating a {@link StatefulFunctionsAppContainers}.
   *
   * @param appName the name of the application.
   * @param numWorkers the number of workers to run the application.
   * @return a builder for creating a {@link StatefulFunctionsAppContainers}.
   */
  public static Builder builder(String appName, int numWorkers) {
    return new Builder(appName, numWorkers);
  }

  @Override
  protected void before() throws Throwable {
    checkpointDir = temporaryCheckpointDir();

    master.withFileSystemBind(
        checkpointDir.getAbsolutePath(), "/checkpoint-dir", BindMode.READ_WRITE);
    workers.forEach(
        worker ->
            worker.withFileSystemBind(
                checkpointDir.getAbsolutePath(), "/checkpoint-dir", BindMode.READ_WRITE));

    master.start();
    workers.forEach(GenericContainer::start);
  }

  @Override
  protected void after() {
    master.stop();
    workers.forEach(GenericContainer::stop);

    FileUtils.deleteDirectoryQuietly(checkpointDir);
  }

  /** @return the exposed port on master for calling REST APIs. */
  public int getMasterRestPort() {
    return master.getMappedPort(8081);
  }

  /**
   * Restarts a single worker of this Stateful Functions application.
   *
   * @param workerIndex the index of the worker to restart.
   */
  public void restartWorker(int workerIndex) {
    if (workerIndex >= workers.size()) {
      throw new IndexOutOfBoundsException(
          "Invalid worker index; valid values are 0 to " + (workers.size() - 1));
    }

    final GenericContainer<?> worker = workers.get(workerIndex);
    worker.stop();
    worker.start();
  }

  private static File temporaryCheckpointDir() throws IOException {
    final Path currentWorkingDir = Paths.get(System.getProperty("user.dir"));
    return Files.createTempDirectory(currentWorkingDir, "statefun-app-checkpoints-").toFile();
  }

  public static final class Builder {
    private static final String JOB_MANAGER_HOST = "statefun-app-master";
    private static final String WORKER_HOST_PREFIX = "statefun-app-worker";

    private final String appName;
    private final int numWorkers;
    private final Network network;

    private final Configuration dynamicProperties = new Configuration();
    private final List<GenericContainer<?>> dependentContainers = new ArrayList<>();
    private final List<ClasspathBuildContextFile> classpathBuildContextFiles = new ArrayList<>();
    private Logger masterLogger;

    private Builder(String appName, int numWorkers) {
      if (appName == null || appName.isEmpty()) {
        throw new IllegalArgumentException(
            "App name must be non-empty. This is used as the application image name.");
      }
      if (numWorkers < 1) {
        throw new IllegalArgumentException("Must have at least 1 worker.");
      }

      this.network = Network.newNetwork();
      this.appName = appName;
      this.numWorkers = numWorkers;
    }

    public StatefulFunctionsAppContainers.Builder dependsOn(GenericContainer<?> container) {
      container.withNetwork(network);
      this.dependentContainers.add(container);
      return this;
    }

    public StatefulFunctionsAppContainers.Builder exposeMasterLogs(Logger logger) {
      this.masterLogger = logger;
      return this;
    }

    public StatefulFunctionsAppContainers.Builder withModuleGlobalConfiguration(
        String key, String value) {
      this.dynamicProperties.setString(StatefulFunctionsConfig.MODULE_CONFIG_PREFIX + key, value);
      return this;
    }

    public <T> StatefulFunctionsAppContainers.Builder withConfiguration(
        ConfigOption<T> config, T value) {
      this.dynamicProperties.set(config, value);
      return this;
    }

    public StatefulFunctionsAppContainers.Builder withBuildContextFileFromClasspath(
        String buildContextPath, String resourcePath) {
      this.classpathBuildContextFiles.add(
          new ClasspathBuildContextFile(buildContextPath, resourcePath));
      return this;
    }

    public StatefulFunctionsAppContainers build() {
      final ImageFromDockerfile appImage =
          appImage(appName, dynamicProperties, classpathBuildContextFiles);

      return new StatefulFunctionsAppContainers(
          masterContainer(appImage, network, dependentContainers, numWorkers, masterLogger),
          workerContainers(appImage, numWorkers, network, masterLogger));
    }

    private static ImageFromDockerfile appImage(
        String appName,
        Configuration dynamicProperties,
        List<ClasspathBuildContextFile> classpathBuildContextFiles) {
      final Path targetDirPath = Paths.get(System.getProperty("user.dir") + "/target/");
      LOG.info("Building app image with built artifacts located at: {}", targetDirPath);

      final ImageFromDockerfile appImage =
          new ImageFromDockerfile(appName)
              .withFileFromClasspath("Dockerfile", "Dockerfile")
              .withFileFromPath(".", targetDirPath);

      Configuration flinkConf = resolveFlinkConf(dynamicProperties);
      String flinkConfString = flinkConfigAsString(flinkConf);
      LOG.info(
          "Resolved Flink configuration after merging dynamic properties with base flink-conf.yaml:\n\n{}",
          flinkConf);
      appImage.withFileFromString("flink-conf.yaml", flinkConfString);

      for (ClasspathBuildContextFile classpathBuildContextFile : classpathBuildContextFiles) {
        appImage.withFileFromClasspath(
            classpathBuildContextFile.buildContextPath, classpathBuildContextFile.fromResourcePath);
      }

      return appImage;
    }

    /**
     * Merges set dynamic properties with configuration in the base flink-conf.yaml located in
     * resources.
     */
    private static Configuration resolveFlinkConf(Configuration dynamicProperties) {
      final InputStream baseFlinkConfResourceInputStream =
          StatefulFunctionsAppContainers.class.getResourceAsStream("/flink-conf.yaml");
      if (baseFlinkConfResourceInputStream == null) {
        throw new RuntimeException("Base flink-conf.yaml cannot be found.");
      }

      final File tempBaseFlinkConfFile = copyToTempFlinkConfFile(baseFlinkConfResourceInputStream);
      return GlobalConfiguration.loadConfiguration(
          tempBaseFlinkConfFile.getParentFile().getAbsolutePath(), dynamicProperties);
    }

    private static String flinkConfigAsString(Configuration configuration) {
      StringBuilder yaml = new StringBuilder();
      for (Map.Entry<String, String> entry : configuration.toMap().entrySet()) {
        yaml.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
      }

      return yaml.toString();
    }

    private static File copyToTempFlinkConfFile(InputStream inputStream) {
      try {
        final File tempFile =
            new File(
                Files.createTempDirectory("statefun-app-containers").toString(), "flink-conf.yaml");
        Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        return tempFile;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private static GenericContainer<?> masterContainer(
        ImageFromDockerfile appImage,
        Network network,
        List<GenericContainer<?>> dependents,
        int numWorkers,
        @Nullable Logger masterLogger) {
      final GenericContainer<?> master =
          new GenericContainer(appImage)
              .withNetwork(network)
              .withNetworkAliases(JOB_MANAGER_HOST)
              .withEnv("JOB_MANAGER_RPC_ADDRESS", JOB_MANAGER_HOST)
              .withCommand("-p " + numWorkers)
              .withCommand("standalone-job")
              .withExposedPorts(8081);

      for (GenericContainer<?> dependent : dependents) {
        master.dependsOn(dependent);
      }

      if (masterLogger != null) {
        master.withLogConsumer(new Slf4jLogConsumer(masterLogger, true));
      }

      return master;
    }

    private static List<GenericContainer<?>> workerContainers(
        ImageFromDockerfile appImage, int numWorkers, Network network, Logger masterLogger) {
      final List<GenericContainer<?>> workers = new ArrayList<>(numWorkers);

      for (int i = 0; i < numWorkers; i++) {
        workers.add(
            new GenericContainer(appImage)
                .withNetwork(network)
                .withNetworkAliases(workerHostOf(i))
                .withEnv("JOB_MANAGER_RPC_ADDRESS", JOB_MANAGER_HOST)
                .withCommand("taskmanager"));
      }

      return workers;
    }

    private static String workerHostOf(int workerIndex) {
      return WORKER_HOST_PREFIX + "-" + workerIndex;
    }

    private static class ClasspathBuildContextFile {
      private final String buildContextPath;
      private final String fromResourcePath;

      ClasspathBuildContextFile(String buildContextPath, String fromResourcePath) {
        this.buildContextPath = Objects.requireNonNull(buildContextPath);
        this.fromResourcePath = Objects.requireNonNull(fromResourcePath);
      }
    }
  }
}
