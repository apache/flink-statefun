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

package org.apache.flink.statefun.e2e.smoke.common;

import org.apache.flink.statefun.e2e.smoke.generated.Commands;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.apache.flink.statefun.e2e.smoke.generated.VerificationResult;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class Types {
  private Types() {}

  public static final TypeName SOURCE_COMMANDS_TYPE =
      TypeName.parseFrom(Constants.NAMESPACE + "/source-command");
  public static final TypeName VERIFICATION_RESULT_TYPE =
      TypeName.parseFrom(Constants.NAMESPACE + "/verification-result");
  public static final TypeName COMMANDS_TYPE =
      TypeName.parseFrom(Constants.NAMESPACE + "/commands");

  public static boolean isTypeOf(TypedValue value, TypeName type) {
    return value.getTypename().equals(type.canonicalTypenameString());
  }

  public static TypedValue packSourceCommand(SourceCommand sourceCommand) {
    return TypedValue.newBuilder()
        .setTypename(SOURCE_COMMANDS_TYPE.canonicalTypenameString())
        .setHasValue(true)
        .setValue(sourceCommand.toByteString())
        .build();
  }

  public static SourceCommand unpackSourceCommand(TypedValue typedValue) {
    if (!isTypeOf(typedValue, SOURCE_COMMANDS_TYPE)) {
      throw new IllegalStateException("Unexpected TypedValue: " + typedValue);
    }
    try {
      return SourceCommand.parseFrom(typedValue.getValue());
    } catch (Exception e) {
      throw new RuntimeException("Unable to parse SourceCommand from TypedValue.", e);
    }
  }

  public static TypedValue packCommands(Commands commands) {
    return TypedValue.newBuilder()
        .setTypename(COMMANDS_TYPE.canonicalTypenameString())
        .setHasValue(true)
        .setValue(commands.toByteString())
        .build();
  }

  public static Commands unpackCommands(TypedValue typedValue) {
    if (!isTypeOf(typedValue, COMMANDS_TYPE)) {
      throw new IllegalStateException("Unexpected TypedValue: " + typedValue);
    }
    try {
      return Commands.parseFrom(typedValue.getValue());
    } catch (Exception e) {
      throw new RuntimeException("Unable to parse Commands from TypedValue.", e);
    }
  }

  public static TypedValue packVerificationResult(VerificationResult verificationResult) {
    return TypedValue.newBuilder()
        .setTypename(VERIFICATION_RESULT_TYPE.canonicalTypenameString())
        .setHasValue(true)
        .setValue(verificationResult.toByteString())
        .build();
  }

  public static VerificationResult unpackVerificationResult(TypedValue typedValue) {
    if (!isTypeOf(typedValue, VERIFICATION_RESULT_TYPE)) {
      throw new IllegalStateException("Unexpected TypedValue: " + typedValue);
    }
    try {
      return VerificationResult.parseFrom(typedValue.getValue());
    } catch (Exception e) {
      throw new RuntimeException("Unable to parse SourceCommand from TypedValue.", e);
    }
  }
}
