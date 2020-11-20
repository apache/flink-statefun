package org.apache.flink.statefun.e2e.smoke;

import org.apache.flink.statefun.e2e.smoke.generated.Command;
import org.apache.flink.statefun.e2e.smoke.generated.Commands;
import org.apache.flink.statefun.e2e.smoke.generated.FunctionTrackerSnapshot;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;

final class FunctionStateTracker {
  private final long[] expectedStates;

  public FunctionStateTracker(int numberOfFunctionInstances) {
    this.expectedStates = new long[numberOfFunctionInstances];
  }

  /**
   * Find any state modification commands nested under @sourceCommand, and apply them in the
   * internal state representation.
   */
  public void apply(SourceCommand sourceCommand) {
    updateInternally(sourceCommand.getTarget(), sourceCommand.getCommands());
  }

  /** Apply all the state modification stored in the snapshot represented by the snapshotBytes. */
  public FunctionStateTracker apply(FunctionTrackerSnapshot snapshot) {
    for (int i = 0; i < snapshot.getStateCount(); i++) {
      expectedStates[i] += snapshot.getState(i);
    }
    return this;
  }

  /** Get the current expected state of a function instance. */
  public long stateOf(int id) {
    return expectedStates[id];
  }

  public FunctionTrackerSnapshot.Builder snapshot() {
    FunctionTrackerSnapshot.Builder snapshot = FunctionTrackerSnapshot.newBuilder();
    for (long state : expectedStates) {
      snapshot.addState(state);
    }
    return snapshot;
  }

  /**
   * Recursively traverse the commands tree and look for {@link Command.IncrementState} commands.
   * For each {@code ModifyState} command found update the corresponding expected state.
   */
  private void updateInternally(int currentAddress, Commands commands) {
    for (Command command : commands.getCommandList()) {
      if (command.hasIncrement()) {
        expectedStates[currentAddress]++;
      } else if (command.hasSend()) {
        updateInternally(command.getSend().getTarget(), command.getSend().getCommands());
      } else if (command.hasSendAfter()) {
        updateInternally(command.getSendAfter().getTarget(), command.getSendAfter().getCommands());
      } else if (command.hasAsyncOperation()) {
        updateInternally(currentAddress, command.getAsyncOperation().getResolvedCommands());
      }
    }
  }
}
