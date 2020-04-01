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

package org.apache.flink.statefun.flink.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.statefun.flink.core.exceptions.StatefulFunctionsInvalidConfigException;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;

public final class StatefulFunctionsConfigValidator {

  private StatefulFunctionsConfigValidator() {}

  public static final List<String> PARENT_FIRST_CLASSLOADER_PATTERNS =
      Collections.unmodifiableList(
          Arrays.asList("org.apache.flink.statefun", "org.apache.kafka", "com.google.protobuf"));

  public static final int MAX_CONCURRENT_CHECKPOINTS = 1;

  static void validate(Configuration configuration) {
    validateParentFirstClassloaderPatterns(configuration);
    validateMaxConcurrentCheckpoints(configuration);
    validateLegacyScheduler(configuration);
  }

  private static void validateParentFirstClassloaderPatterns(Configuration configuration) {
    final Set<String> parentFirstClassloaderPatterns =
        parentFirstClassloaderPatterns(configuration);
    if (!parentFirstClassloaderPatterns.containsAll(PARENT_FIRST_CLASSLOADER_PATTERNS)) {
      throw new StatefulFunctionsInvalidConfigException(
          CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
          "Must contain all of " + String.join(", ", PARENT_FIRST_CLASSLOADER_PATTERNS));
    }
  }

  private static void validateMaxConcurrentCheckpoints(Configuration configuration) {
    final int maxConcurrentCheckpoints =
        configuration.get(ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS);
    if (maxConcurrentCheckpoints != 1) {
      throw new StatefulFunctionsInvalidConfigException(
          ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS,
          "Value must be 1, Stateful Functions does not support concurrent checkpoints.");
    }
  }

  private static void validateLegacyScheduler(Configuration configuration) {
    String configuredScheduler = configuration.get(JobManagerOptions.SCHEDULER);
    if (!"legacy".equalsIgnoreCase(configuredScheduler)) {
      throw new StatefulFunctionsInvalidConfigException(
          JobManagerOptions.SCHEDULER, "Currently the only supported scheduler is 'legacy'");
    }
  }

  private static Set<String> parentFirstClassloaderPatterns(Configuration configuration) {
    final String[] split =
        configuration.get(CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL).split(";");
    final Set<String> parentFirstClassloaderPatterns = new HashSet<>(split.length);
    for (String s : split) {
      parentFirstClassloaderPatterns.add(s.trim().toLowerCase(Locale.ENGLISH));
    }
    return parentFirstClassloaderPatterns;
  }
}
