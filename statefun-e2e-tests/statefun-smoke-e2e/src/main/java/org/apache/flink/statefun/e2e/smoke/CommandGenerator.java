package org.apache.flink.statefun.e2e.smoke;

import static java.util.Arrays.asList;
import static org.apache.commons.math3.util.Pair.create;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.statefun.e2e.smoke.generated.Command;
import org.apache.flink.statefun.e2e.smoke.generated.Commands;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;

/**
 * Generates random commands to be interpreted by {@linkplain CommandInterpreter}.
 *
 * <p>see {src/main/protobuf/commands.proto}
 */
public final class CommandGenerator implements Supplier<SourceCommand> {

  private final RandomGenerator random;
  private final EnumeratedDistribution<Gen> distribution;
  private final ModuleParameters moduleParameters;

  public CommandGenerator(RandomGenerator random, ModuleParameters parameters) {
    this.random = Objects.requireNonNull(random);
    this.moduleParameters = Objects.requireNonNull(parameters);
    this.distribution = new EnumeratedDistribution<>(random, randomCommandGenerators());
  }

  @Override
  public SourceCommand get() {
    final int depth = random.nextInt(moduleParameters.getCommandDepth());
    return SourceCommand.newBuilder().setTarget(address()).setCommands(commands(depth)).build();
  }

  private Commands.Builder commands(int depth) {
    Commands.Builder builder = Commands.newBuilder();
    if (depth <= 0) {
      StateModifyGen.instance().generate(builder, depth);
      return builder;
    }
    final int n = random.nextInt(moduleParameters.getMaxCommandsPerDepth());
    for (int i = 0; i < n; i++) {
      Gen gen = distribution.sample();
      gen.generate(builder, depth);
    }
    if (builder.getCommandCount() == 0) {
      StateModifyGen.instance().generate(builder, depth);
    }
    return builder;
  }

  private int address() {
    return random.nextInt(moduleParameters.getNumberOfFunctionInstances());
  }

  private List<Pair<Gen, Double>> randomCommandGenerators() {
    return asList(
        create(new StateModifyGen(), moduleParameters.getStateModificationsPr()),
        create(new SendGen(), moduleParameters.getSendPr()),
        create(new SendAfterGen(), moduleParameters.getSendAfterPr()),
        create(new SendAsyncOp(), moduleParameters.getAsyncSendPr()),
        create(new Noop(), moduleParameters.getNoopPr()),
        create(new SendEgress(), moduleParameters.getSendEgressPr()));
  }

  interface Gen {
    /** generates one or more commands with depth at most @depth. */
    void generate(Commands.Builder builder, int depth);
  }

  // ----------------------------------------------------------------------------------------------------
  // generators
  // ----------------------------------------------------------------------------------------------------

  private static final class SendEgress implements Gen {

    @Override
    public void generate(Commands.Builder builder, int depth) {
      builder.addCommand(
          Command.newBuilder().setSendEgress(Command.SendEgress.getDefaultInstance()));
    }
  }

  private static final class Noop implements Gen {
    @Override
    public void generate(Commands.Builder builder, int depth) {}
  }

  private static final class StateModifyGen implements Gen {

    static final Gen INSTANCE = new StateModifyGen();

    static Gen instance() {
      return INSTANCE;
    }

    @Override
    public void generate(Commands.Builder builder, int depth) {
      builder.addCommand(
          Command.newBuilder().setIncrement(Command.IncrementState.getDefaultInstance()));
    }
  }

  private final class SendAfterGen implements Gen {

    @Override
    public void generate(Commands.Builder builder, int depth) {
      builder.addCommand(Command.newBuilder().setSendAfter(sendAfter(depth)));
    }

    private Command.SendAfter.Builder sendAfter(int depth) {
      return Command.SendAfter.newBuilder().setTarget(address()).setCommands(commands(depth - 1));
    }
  }

  private final class SendGen implements Gen {

    @Override
    public void generate(Commands.Builder builder, int depth) {
      builder.addCommand(Command.newBuilder().setSend(send(depth)));
    }

    private Command.Send.Builder send(int depth) {
      return Command.Send.newBuilder().setTarget(address()).setCommands(commands(depth - 1));
    }
  }

  private final class SendAsyncOp implements Gen {

    @Override
    public void generate(Commands.Builder builder, int depth) {
      builder.addCommand(Command.newBuilder().setAsyncOperation(asyncOp(depth)));
    }

    private Command.AsyncOperation.Builder asyncOp(int depth) {
      return Command.AsyncOperation.newBuilder()
          .setFailure(random.nextBoolean())
          .setResolvedCommands(commands(depth - 1));
    }
  }
}
