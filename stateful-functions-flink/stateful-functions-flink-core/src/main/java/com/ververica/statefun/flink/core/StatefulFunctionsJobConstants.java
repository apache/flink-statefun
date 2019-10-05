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

package com.ververica.statefun.flink.core;

import com.ververica.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

@SuppressWarnings("WeakerAccess")
public final class StatefulFunctionsJobConstants {

  public static final String FUNCTION_OPERATOR_NAME = "functions";
  public static final String FUNCTION_OPERATOR_UID = "functions_uid1";
  public static final String WRITE_BACK_OPERATOR_NAME = "feedback";
  public static final String WRITE_BACK_OPERATOR_UID = "feedback_uid1";
  public static final String ROUTER_NAME = "router";

  @Documentation.ExcludeFromDocumentation("internal configuration")
  public static final ConfigOption<byte[]> STATEFUL_FUNCTIONS_UNIVERSE_INITIALIZER_CLASS_BYTES =
      ConfigOptions.key("stateful-functions.internal.core.universe-serialized-initializer")
          .defaultValue(new byte[0]);

  public static final ConfigOption<Integer> TOTAL_MEMORY_USED_FOR_FEEDBACK_CHECKPOINTING =
      ConfigOptions.key("stateful-functions.feedback.memory.bytes")
          .defaultValue(32 * 1024 * 1024)
          .withDescription(
              "The number of bytes to use for in memory buffering of the feedback channel, before spilling to disk");

  public static final ConfigOption<Long> CHECKPOINTING_INTERVAL =
      ConfigOptions.key("stateful-functions.state.checkpointing-interval-ms")
          .defaultValue(30 * 1000L)
          .withDescription(
              "Flink checkpoint interval in milliseconds, set to -1 to disable checkpointing");

  public static final ConfigOption<Boolean> MULTIPLEX_FLINK_STATE =
      ConfigOptions.key("stateful-functions.state.multiplex-flink-state")
          .defaultValue(true)
          .withDescription(
              "Use a single MapState to multiplex different function types and persisted values,"
                  + "instead of using a ValueState for each <FunctionType, PersistedValue> combination");

  public static final ConfigOption<String> USER_MESSAGE_SERIALIZER =
      ConfigOptions.key("stateful-functions.message.serializer")
          .defaultValue(MessageFactoryType.WITH_PROTOBUF_PAYLOADS.name())
          .withDescription("The serializer to use for on the wire messages.");

  public static final ConfigOption<String> FLINK_JOB_NAME =
      ConfigOptions.key("stateful-functions.flink-job-name")
          .defaultValue("StatefulFunctions")
          .withDescription("The name to display at the Flink-UI");

  private StatefulFunctionsJobConstants() {}
}
