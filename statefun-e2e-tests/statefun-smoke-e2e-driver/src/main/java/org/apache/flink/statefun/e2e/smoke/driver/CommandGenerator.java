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
package org.apache.flink.statefun.e2e.smoke.driver;

import static java.util.Arrays.asList;
import static org.apache.commons.math3.util.Pair.create;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.statefun.e2e.smoke.SmokeRunnerParameters;
import org.apache.flink.statefun.e2e.smoke.generated.Command;
import org.apache.flink.statefun.e2e.smoke.generated.Commands;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;

/**
 * Generates random commands to be interpreted by functions of type {@link Constants#FN_TYPE}.
 *
 * <p>see {src/main/protobuf/commands.proto}
 */
public final class CommandGenerator implements Supplier<SourceCommand> {

  private final RandomGenerator random;
  private final EnumeratedDistribution<Gen> distribution;
  private final SmokeRunnerParameters parameters;

  public CommandGenerator(RandomGenerator random, SmokeRunnerParameters parameters) {
    this.random = Objects.requireNonNull(random);
    this.parameters = Objects.requireNonNull(parameters);
    this.distribution = new EnumeratedDistribution<>(random, randomCommandGenerators());
  }

  @Override
  public SourceCommand get() {
    final int depth = random.nextInt(parameters.getCommandDepth());
    return SourceCommand.newBuilder().setTarget(address()).setCommands(commands(depth)).build();
  }

  private Commands.Builder commands(int depth) {
    Commands.Builder builder = Commands.newBuilder();
    if (depth <= 0) {
      StateModifyGen.instance().generate(builder, depth);
      return builder;
    }
    final int n = random.nextInt(parameters.getMaxCommandsPerDepth());
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
    return random.nextInt(parameters.getNumberOfFunctionInstances());
  }

  private List<Pair<Gen, Double>> randomCommandGenerators() {
    List<Pair<Gen, Double>> list =
        new ArrayList<>(
            asList(
                create(new StateModifyGen(), parameters.getStateModificationsPr()),
                create(new SendGen(), parameters.getSendPr()),
                create(new SendAfterGen(), parameters.getSendAfterPr()),
                create(new Noop(), parameters.getNoopPr()),
                create(new SendEgress(), parameters.getSendEgressPr())));

    if (parameters.isAsyncOpSupported()) {
      list.add(create(new SendAsyncOp(), parameters.getAsyncSendPr()));
    }
    if (parameters.isDelayCancellationOpSupported()) {
      list.add(create(new SendAfterCancellationGen(), parameters.getSendAfterWithCancellationPr()));
    }
    return list;
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

  private final class SendAfterCancellationGen implements Gen {

    @Override
    public void generate(Commands.Builder builder, int depth) {
      final String token = new UUID(random.nextLong(), random.nextLong()).toString();
      final int address = address();

      Command.SendAfter.Builder first =
          Command.SendAfter.newBuilder().setTarget(address).setCancellationToken(token);
      Command.CancelSendAfter.Builder second =
          Command.CancelSendAfter.newBuilder().setTarget(address).setCancellationToken(token);

      builder.addCommand(Command.newBuilder().setSendAfter(first));
      builder.addCommand(Command.newBuilder().setCancelSendAfter(second));
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
