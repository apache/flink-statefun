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

package org.apache.flink.statefun.e2e.smoke.java;

import org.apache.flink.statefun.e2e.smoke.generated.Commands;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.apache.flink.statefun.e2e.smoke.generated.VerificationResult;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

final class Constants {
  private Constants() {}

  private static final String APP_NAMESPACE = "statefun.smoke.e2e";

  // =====================================================
  //  Egresses
  // =====================================================

  static final TypeName DISCARD_EGRESS = TypeName.typeNameOf(APP_NAMESPACE, "discard-sink");

  static final TypeName VERIFICATION_EGRESS =
      TypeName.typeNameOf(APP_NAMESPACE, "verification-sink");

  // =====================================================
  //  This function
  // =====================================================

  static final TypeName CMD_INTERPRETER_FN =
      TypeName.typeNameOf(APP_NAMESPACE, "command-interpreter-fn");

  // =====================================================
  //  Command types
  // =====================================================

  static final Type<Commands> COMMANDS_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameOf(APP_NAMESPACE, "commands"),
          Commands::toByteArray,
          Commands::parseFrom);

  static final Type<SourceCommand> SOURCE_COMMAND_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameOf(APP_NAMESPACE, "source-command"),
          SourceCommand::toByteArray,
          SourceCommand::parseFrom);

  static final Type<VerificationResult> VERIFICATION_RESULT_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameOf(APP_NAMESPACE, "verification-result"),
          VerificationResult::toByteArray,
          VerificationResult::parseFrom);
}
