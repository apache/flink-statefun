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

import static org.apache.flink.client.cli.CliFrontendParser.PARALLELISM_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.CONFIG_DIR_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.DYNAMIC_PROPERTY_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.HOST_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.REST_PORT_OPTION;

import java.util.Properties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

/**
 * Parser factory which generates a {@link StatefulFunctionsClusterConfiguration} from a given list
 * of command line arguments.
 *
 * <p>This class was copied from Apache Flink.
 */
public class StatefulFunctionsClusterConfigurationParserFactory
    implements ParserResultFactory<StatefulFunctionsClusterConfiguration> {

  private static final Option JOB_ID_OPTION =
      Option.builder("jid")
          .longOpt("job-id")
          .required(false)
          .hasArg(true)
          .argName("job id")
          .desc("Job ID of the job to run.")
          .build();

  @Nullable
  private static JobID getJobId(CommandLine commandLine) throws FlinkParseException {
    String jobId = commandLine.getOptionValue(JOB_ID_OPTION.getOpt());
    if (jobId == null) {
      return null;
    }
    try {
      return JobID.fromHexString(jobId);
    } catch (IllegalArgumentException e) {
      throw createFlinkParseException(JOB_ID_OPTION, e);
    }
  }

  private static FlinkParseException createFlinkParseException(Option option, Exception cause) {
    return new FlinkParseException(
        String.format("Failed to parse '--%s' option", option.getLongOpt()), cause);
  }

  @Override
  public Options getOptions() {
    final Options options = new Options();
    options.addOption(CONFIG_DIR_OPTION);
    options.addOption(REST_PORT_OPTION);
    options.addOption(JOB_ID_OPTION);
    options.addOption(DYNAMIC_PROPERTY_OPTION);
    options.addOption(PARALLELISM_OPTION);
    options.addOption(CliFrontendParser.SAVEPOINT_PATH_OPTION);
    options.addOption(CliFrontendParser.SAVEPOINT_ALLOW_NON_RESTORED_OPTION);

    return options;
  }

  @Override
  public StatefulFunctionsClusterConfiguration createResult(@Nonnull CommandLine commandLine)
      throws FlinkParseException {
    final String configDir = commandLine.getOptionValue(CONFIG_DIR_OPTION.getOpt());
    final Properties dynamicProperties =
        commandLine.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());
    final int restPort = getRestPort(commandLine);
    final String hostname = commandLine.getOptionValue(HOST_OPTION.getOpt());
    final int parallelism = getParallelism(commandLine);
    final SavepointRestoreSettings savepointRestoreSettings =
        CliFrontendParser.createSavepointRestoreSettings(commandLine);
    final JobID jobId = getJobId(commandLine);

    return new StatefulFunctionsClusterConfiguration(
        configDir,
        dynamicProperties,
        commandLine.getArgs(),
        hostname,
        restPort,
        savepointRestoreSettings,
        jobId,
        parallelism);
  }

  private int getRestPort(CommandLine commandLine) throws FlinkParseException {
    final String restPortString = commandLine.getOptionValue(REST_PORT_OPTION.getOpt(), "-1");
    try {
      return Integer.parseInt(restPortString);
    } catch (NumberFormatException e) {
      throw createFlinkParseException(REST_PORT_OPTION, e);
    }
  }

  private int getParallelism(CommandLine commandLine) throws FlinkParseException {
    final String parallelismString =
        commandLine.getOptionValue(
            PARALLELISM_OPTION.getOpt(), String.valueOf(ExecutionConfig.PARALLELISM_DEFAULT));
    try {
      return Integer.parseInt(parallelismString);
    } catch (NumberFormatException e) {
      throw createFlinkParseException(PARALLELISM_OPTION, e);
    }
  }
}
