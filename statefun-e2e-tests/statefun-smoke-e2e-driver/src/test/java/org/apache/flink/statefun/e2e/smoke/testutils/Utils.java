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
package org.apache.flink.statefun.e2e.smoke.testutils;

import org.apache.flink.statefun.e2e.smoke.generated.Command;
import org.apache.flink.statefun.e2e.smoke.generated.Commands;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;

public class Utils {

  public static SourceCommand aStateModificationCommand() {
    return aStateModificationCommand(-1234); // the id doesn't matter
  }

  public static SourceCommand aStateModificationCommand(int functionInstanceId) {
    return SourceCommand.newBuilder()
        .setTarget(functionInstanceId)
        .setCommands(Commands.newBuilder().addCommand(modify()))
        .build();
  }

  public static SourceCommand aRelayedStateModificationCommand(
      int firstFunctionId, int secondFunctionId) {
    return SourceCommand.newBuilder()
        .setTarget(firstFunctionId)
        .setCommands(Commands.newBuilder().addCommand(sendTo(secondFunctionId, modify())))
        .build();
  }

  private static Command.Builder sendTo(int id, Command.Builder body) {
    return Command.newBuilder()
        .setSend(
            Command.Send.newBuilder()
                .setTarget(id)
                .setCommands(Commands.newBuilder().addCommand(body)));
  }

  private static Command.Builder modify() {
    return Command.newBuilder().setIncrement(Command.IncrementState.getDefaultInstance());
  }
}
