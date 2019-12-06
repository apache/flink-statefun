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

package com.ververica.statefun.flink.launcher;

import static java.util.Objects.requireNonNull;

import com.ververica.statefun.flink.core.StatefulFunctionsJob;
import com.ververica.statefun.flink.core.spi.Constants;
import com.ververica.statefun.flink.core.spi.ModuleSpecs;
import com.ververica.statefun.flink.core.spi.ModuleSpecs.ModuleSpec;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.entrypoint.component.JobGraphRetriever;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link JobGraphRetriever} which creates the {@link JobGraph} from a class on the class path.
 *
 * <p>This class is based on a class present in Apache Flink but it sets the correct class path for
 * the child first classloader.
 */
final class StatefulFunctionsJobGraphRetriever implements JobGraphRetriever {

  private static final Logger LOG =
      LoggerFactory.getLogger(StatefulFunctionsJobGraphRetriever.class);

  private final JobID jobId;
  private final SavepointRestoreSettings savepointRestoreSettings;
  private final String[] programArguments;

  StatefulFunctionsJobGraphRetriever(
      JobID jobId, SavepointRestoreSettings savepointRestoreSettings, String[] programArguments) {
    this.jobId = requireNonNull(jobId, "jobId");
    this.savepointRestoreSettings =
        requireNonNull(savepointRestoreSettings, "savepointRestoreSettings");
    this.programArguments = requireNonNull(programArguments, "programArguments");
  }

  private static List<URL> obtainModuleAdditionalClassPath() {
    try {
      ModuleSpecs specs = ModuleSpecs.fromPath(Constants.MODULE_DIRECTORY);
      List<URL> classPath = new ArrayList<>();
      for (ModuleSpec spec : specs) {
        for (URI uri : spec.artifactUris()) {
          classPath.add(uri.toURL());
        }
      }
      return classPath;
    } catch (IOException e) {
      throw new RuntimeException(
          "Unable to load modules from path " + Constants.MODULE_DIRECTORY, e);
    }
  }

  @Override
  public JobGraph retrieveJobGraph(Configuration configuration) throws FlinkException {
    final PackagedProgram packagedProgram = createPackagedProgram();

    final int defaultParallelism = configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
    try {
      final JobGraph jobGraph =
          PackagedProgramUtils.createJobGraph(
              packagedProgram, configuration, defaultParallelism, jobId);
      jobGraph.setSavepointRestoreSettings(savepointRestoreSettings);

      return jobGraph;
    } catch (Exception e) {
      throw new FlinkException("Could not create the JobGraph from the provided user code jar.", e);
    }
  }

  private PackagedProgram createPackagedProgram() {
    File mainJar = new File(Constants.FLINK_JOB_JAR_PATH);
    if (!mainJar.exists()) {
      throw new IllegalStateException("Unable to locate the launcher jar");
    }
    try {
      return PackagedProgram.newBuilder()
          .setJarFile(mainJar)
          .setUserClassPaths(obtainModuleAdditionalClassPath())
          .setEntryPointClassName(StatefulFunctionsJob.class.getName())
          .setArguments(programArguments)
          .build();
    } catch (ProgramInvocationException e) {
      throw new RuntimeException("Unable to construct a packaged program", e);
    }
  }
}
