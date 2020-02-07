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

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;

@SuppressWarnings("WeakerAccess")
public final class StatefulFunctionsJobConstants {

  public static final String FEEDBACK_UNION_OPERATOR_NAME = "feedback-union";
  public static final String FEEDBACK_UNION_OPERATOR_UID = "feedback_union_uid1";
  public static final String FUNCTION_OPERATOR_NAME = "functions";
  public static final String FUNCTION_OPERATOR_UID = "functions_uid1";
  public static final String WRITE_BACK_OPERATOR_NAME = "feedback";
  public static final String WRITE_BACK_OPERATOR_UID = "feedback_uid1";
  public static final String ROUTER_NAME = "router";

  @Documentation.ExcludeFromDocumentation("internal configuration")
  public static final ConfigOption<byte[]> STATEFUL_FUNCTIONS_UNIVERSE_INITIALIZER_CLASS_BYTES =
      ConfigOptions.key("statefun.internal.core.universe-serialized-initializer")
          .defaultValue(new byte[0]);

  public static final ConfigOption<Long> CHECKPOINTING_INTERVAL =
      ConfigOptions.key("statefun.state.checkpointing-interval-ms")
          .defaultValue(30 * 1000L)
          .withDescription(
              "Flink checkpoint interval in milliseconds, set to -1 to disable checkpointing");

  public static final ConfigOption<String> USER_MESSAGE_SERIALIZER =
      ConfigOptions.key("statefun.message.serializer")
          .defaultValue(MessageFactoryType.WITH_PROTOBUF_PAYLOADS.name())
          .withDescription("The serializer to use for on the wire messages.");

  public static final ConfigOption<String> FLINK_JOB_NAME =
      ConfigOptions.key("statefun.flink-job-name")
          .defaultValue("StatefulFunctions")
          .withDescription("The name to display at the Flink-UI");

  private StatefulFunctionsJobConstants() {}
}
