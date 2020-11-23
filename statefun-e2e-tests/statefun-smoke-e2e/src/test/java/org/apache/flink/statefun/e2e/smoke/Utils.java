package org.apache.flink.statefun.e2e.smoke;

import com.google.protobuf.Any;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.flink.statefun.e2e.smoke.generated.Command;
import org.apache.flink.statefun.e2e.smoke.generated.Commands;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.apache.flink.statefun.e2e.smoke.generated.VerificationResult;

class Utils {

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

  /** Blocks the currently executing thread until enough successful verification results supply. */
  static void awaitVerificationSuccess(Supplier<Any> results, final int numberOfFunctionInstances) {
    Set<Integer> successfullyVerified = new HashSet<>();
    while (successfullyVerified.size() != numberOfFunctionInstances) {
      Any any = results.get();
      VerificationResult result = ProtobufUtils.unpack(any, VerificationResult.class);
      if (result.getActual() == result.getExpected()) {
        successfullyVerified.add(result.getId());
      } else if (result.getActual() > result.getExpected()) {
        throw new AssertionError(
            "Over counted. Expected: "
                + result.getExpected()
                + ", actual: "
                + result.getActual()
                + ", function: "
                + result.getId());
      }
    }
  }

  /** starts a simple Protobuf TCP server that accepts {@link com.google.protobuf.Any}. */
  static SimpleProtobufServer.StartedServer<Any> startProtobufServer() {
    SimpleProtobufServer<Any> server = new SimpleProtobufServer<>(Any.parser());
    return server.start();
  }
}
