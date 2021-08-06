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

package org.apache.flink.statefun.e2e.smoke;

import java.time.Duration;
import java.util.Objects;
import org.apache.flink.statefun.e2e.smoke.generated.Command;
import org.apache.flink.statefun.e2e.smoke.generated.Commands;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.apache.flink.statefun.e2e.smoke.generated.VerificationResult;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageBuilder;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

public final class CommandInterpreter {
  private final Ids ids;
  private static final Duration sendAfterDelay = Duration.ofMillis(1);

  static final Type<Command> COMMAND_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameOf(Constants.PROTOBUF_NAMESPACE, Command.getDescriptor().getFullName()),
          Command::toByteArray,
          Command::parseFrom);

  static final Type<Commands> COMMANDS_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameOf(Constants.PROTOBUF_NAMESPACE, Commands.getDescriptor().getFullName()),
          Commands::toByteArray,
          Commands::parseFrom);

  static final Type<SourceCommand> SOURCE_COMMAND_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameOf(
              Constants.PROTOBUF_NAMESPACE, SourceCommand.getDescriptor().getFullName()),
          SourceCommand::toByteArray,
          SourceCommand::parseFrom);

  static final Type<VerificationResult> VERIFICATION_RESULT_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameOf(
              Constants.PROTOBUF_NAMESPACE, VerificationResult.getDescriptor().getFullName()),
          VerificationResult::toByteArray,
          VerificationResult::parseFrom);

  static final TypeName DISCARD_EGRESS_TYPENAME =
      TypeName.typeNameOf(Constants.NAMESPACE, Constants.EGRESS_NAME);

  static final TypeName VERIFICATION_EGRESS_TYPENAME =
      TypeName.typeNameOf(Constants.NAMESPACE, Constants.VERIFICATION_EGRESS_NAME);

  public CommandInterpreter(Ids ids) {
    this.ids = Objects.requireNonNull(ids);
  }

  public void interpret(ValueSpec<Long> state, Context context, Message message) {
    if (message.is(SOURCE_COMMAND_TYPE)) {
      interpret(state, context, message.as(SOURCE_COMMAND_TYPE).getCommands());
    } else if (message.is(COMMANDS_TYPE)) {
      interpret(state, context, message.as(COMMANDS_TYPE));
    } else {
      throw new IllegalArgumentException("Unrecognized message type " + message.valueTypeName());
    }
  }

  private void interpret(ValueSpec<Long> state, Context context, Commands cmds) {
    for (Command cmd : cmds.getCommandList()) {
      if (cmd.hasIncrement()) {
        modifyState(state, context, cmd.getIncrement());
      } else if (cmd.hasSend()) {
        send(state, context, cmd.getSend());
      } else if (cmd.hasSendAfter()) {
        sendAfter(state, context, cmd.getSendAfter());
      } else if (cmd.hasSendEgress()) {
        sendEgress(state, context, cmd.getSendEgress());
      } else if (cmd.hasVerify()) {
        verify(state, context, cmd.getVerify());
      }
    }
  }

  private void verify(
      ValueSpec<Long> state, @SuppressWarnings("unused") Context context, Command.Verify verify) {
    AddressScopedStorage storage = context.storage();
    int selfId = Integer.parseInt(context.self().id());
    long actual = storage.get(state).orElse(0L);
    long expected = verify.getExpected();
    VerificationResult verificationResult =
        VerificationResult.newBuilder()
            .setId(selfId)
            .setActual(actual)
            .setExpected(expected)
            .build();
    EgressMessage egressMessage =
        EgressMessageBuilder.forEgress(VERIFICATION_EGRESS_TYPENAME)
            .withCustomType(VERIFICATION_RESULT_TYPE, verificationResult)
            .build();
    context.send(egressMessage);
  }

  private void sendEgress(
      @SuppressWarnings("unused") ValueSpec<Long> state,
      Context context,
      @SuppressWarnings("unused") Command.SendEgress sendEgress) {
    EgressMessage egressMessage =
        EgressMessageBuilder.forEgress(DISCARD_EGRESS_TYPENAME)
            .withCustomType(COMMAND_TYPE, Command.getDefaultInstance())
            .build();
    context.send(egressMessage);
  }

  private void sendAfter(
      @SuppressWarnings("unused") ValueSpec<Long> state, Context context, Command.SendAfter send) {
    String id = ids.idOf(send.getTarget());
    Address targetAddress = new Address(CommandInterpreterFn.TYPENAME, id);
    Message message =
        MessageBuilder.forAddress(targetAddress)
            .withCustomType(COMMANDS_TYPE, send.getCommands())
            .build();
    context.sendAfter(sendAfterDelay, message);
  }

  private void send(
      @SuppressWarnings("unused") ValueSpec<Long> state, Context context, Command.Send send) {
    String id = ids.idOf(send.getTarget());
    Address targetAddress = new Address(CommandInterpreterFn.TYPENAME, id);
    Message message =
        MessageBuilder.forAddress(targetAddress)
            .withCustomType(COMMANDS_TYPE, send.getCommands())
            .build();
    context.send(message);
  }

  private void modifyState(
      ValueSpec<Long> state,
      @SuppressWarnings("unused") Context context,
      @SuppressWarnings("unused") Command.IncrementState incrementState) {
    AddressScopedStorage storage = context.storage();
    long n = storage.get(state).orElse(0L);
    storage.set(state, n + 1);
  }
}
