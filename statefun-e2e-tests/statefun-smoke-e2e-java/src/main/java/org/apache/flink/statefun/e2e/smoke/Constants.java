package org.apache.flink.statefun.e2e.smoke;

import org.apache.flink.statefun.e2e.smoke.generated.Command;
import org.apache.flink.statefun.e2e.smoke.generated.Commands;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.apache.flink.statefun.e2e.smoke.generated.VerificationResult;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

final class Constants {
  private Constants() {}

  private static final String APP_NAMESPACE = "statefun.smoke.e2e";
  private static final String PROTO_TYPES_NAMESPACE = "type.googleapis.com";

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

  static final Type<Command> COMMAND_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameOf(PROTO_TYPES_NAMESPACE, Command.getDescriptor().getFullName()),
          Command::toByteArray,
          Command::parseFrom);

  static final Type<Commands> COMMANDS_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameOf(PROTO_TYPES_NAMESPACE, Commands.getDescriptor().getFullName()),
          Commands::toByteArray,
          Commands::parseFrom);

  static final Type<SourceCommand> SOURCE_COMMAND_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameOf(PROTO_TYPES_NAMESPACE, SourceCommand.getDescriptor().getFullName()),
          SourceCommand::toByteArray,
          SourceCommand::parseFrom);

  static final Type<VerificationResult> VERIFICATION_RESULT_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameOf(
              PROTO_TYPES_NAMESPACE, VerificationResult.getDescriptor().getFullName()),
          VerificationResult::toByteArray,
          VerificationResult::parseFrom);
}
