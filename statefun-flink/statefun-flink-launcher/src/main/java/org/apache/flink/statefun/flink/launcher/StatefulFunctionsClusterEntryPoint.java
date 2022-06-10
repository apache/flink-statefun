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
package org.apache.flink.statefun.flink.launcher;

import static java.util.Objects.requireNonNull;

import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.statefun.flink.core.spi.Constants;

/** {@link JobClusterEntrypoint} which is started with a job in a predefined location. */
public final class StatefulFunctionsClusterEntryPoint extends JobClusterEntrypoint {

  public static final JobID ZERO_JOB_ID = new JobID(0, 0);

  @Nonnull private final JobID jobId;

  @Nonnull private final SavepointRestoreSettings savepointRestoreSettings;

  private final int parallelism;

  @Nonnull private final String[] programArguments;

  private StatefulFunctionsClusterEntryPoint(
      Configuration configuration,
      @Nonnull JobID jobId,
      @Nonnull SavepointRestoreSettings savepointRestoreSettings,
      int parallelism,
      @Nonnull String[] programArguments) {
    super(configuration);
    this.jobId = requireNonNull(jobId, "jobId");
    this.savepointRestoreSettings =
        requireNonNull(savepointRestoreSettings, "savepointRestoreSettings");
    this.programArguments = requireNonNull(programArguments, "programArguments");
    this.parallelism = parallelism;
  }

  public static void main(String[] args) {
    EnvironmentInformation.logEnvironmentInfo(
        LOG, StatefulFunctionsClusterEntryPoint.class.getSimpleName(), args);
    SignalHandler.register(LOG);
    JvmShutdownSafeguard.installAsShutdownHook(LOG);

    final CommandLineParser<StatefulFunctionsClusterConfiguration> commandLineParser =
        new CommandLineParser<>(new StatefulFunctionsClusterConfigurationParserFactory());
    StatefulFunctionsClusterConfiguration clusterConfiguration = null;

    try {
      clusterConfiguration = commandLineParser.parse(args);
    } catch (Exception e) {
      LOG.error("Could not parse command line arguments {}.", args, e);
      commandLineParser.printHelp(StatefulFunctionsClusterEntryPoint.class.getSimpleName());
      System.exit(1);
    }

    Configuration configuration = loadConfiguration(clusterConfiguration);
    addStatefulFunctionsConfiguration(configuration);
    setDefaultExecutionModeIfNotConfigured(configuration);

    StatefulFunctionsClusterEntryPoint entrypoint =
        new StatefulFunctionsClusterEntryPoint(
            configuration,
            resolveJobIdForCluster(
                Optional.ofNullable(clusterConfiguration.getJobId()), configuration),
            clusterConfiguration.getSavepointRestoreSettings(),
            clusterConfiguration.getParallelism(),
            clusterConfiguration.getArgs());

    ClusterEntrypoint.runClusterEntrypoint(entrypoint);
  }

  @VisibleForTesting
  @Nonnull
  static JobID resolveJobIdForCluster(Optional<JobID> optionalJobID, Configuration configuration) {
    return optionalJobID.orElseGet(() -> createJobIdForCluster(configuration));
  }

  @Nonnull
  private static JobID createJobIdForCluster(Configuration globalConfiguration) {
    if (HighAvailabilityMode.isHighAvailabilityModeActivated(globalConfiguration)) {
      return ZERO_JOB_ID;
    } else {
      return JobID.generate();
    }
  }

  @VisibleForTesting
  static void setDefaultExecutionModeIfNotConfigured(Configuration configuration) {
    if (isNoExecutionModeConfigured(configuration)) {
      // In contrast to other places, the default for standalone job clusters is
      // ExecutionMode.DETACHED
      configuration.setString(
          ClusterEntrypoint.INTERNAL_CLUSTER_EXECUTION_MODE, ExecutionMode.DETACHED.toString());
    }
  }

  private static boolean isNoExecutionModeConfigured(Configuration configuration) {
    return configuration.getString(ClusterEntrypoint.INTERNAL_CLUSTER_EXECUTION_MODE, null) == null;
  }

  private static void addStatefulFunctionsConfiguration(Configuration configuration) {
    String parentFirst =
        configuration.getString(CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL, "");
    if (parentFirst.isEmpty()) {
      parentFirst = Constants.STATEFUL_FUNCTIONS_PACKAGE;
    } else if (parentFirst.endsWith(";")) {
      parentFirst = parentFirst + Constants.STATEFUL_FUNCTIONS_PACKAGE;
    } else {
      parentFirst = parentFirst + ";" + Constants.STATEFUL_FUNCTIONS_PACKAGE;
    }
    configuration.setString(
        CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL, parentFirst);
  }

  @Override
  protected DispatcherResourceManagerComponentFactory
      createDispatcherResourceManagerComponentFactory(Configuration configuration) {
    return DefaultDispatcherResourceManagerComponentFactory.createJobComponentFactory(
        StandaloneResourceManagerFactory.getInstance(),
        new StatefulFunctionsJobGraphRetriever(
            jobId, savepointRestoreSettings, parallelism, programArguments));
  }
}
