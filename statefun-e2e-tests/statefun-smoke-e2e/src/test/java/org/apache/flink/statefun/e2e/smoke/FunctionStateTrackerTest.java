package org.apache.flink.statefun.e2e.smoke;

import static org.apache.flink.statefun.e2e.smoke.Utils.aRelayedStateModificationCommand;
import static org.apache.flink.statefun.e2e.smoke.Utils.aStateModificationCommand;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class FunctionStateTrackerTest {

  @Test
  public void exampleUsage() {
    FunctionStateTracker tracker = new FunctionStateTracker(1_000);

    tracker.apply(aStateModificationCommand(5));
    tracker.apply(aStateModificationCommand(5));
    tracker.apply(aStateModificationCommand(5));

    assertThat(tracker.stateOf(5), is(3L));
  }

  @Test
  public void testRelay() {
    FunctionStateTracker tracker = new FunctionStateTracker(1_000);

    // send a layered state increment message, first to function 5, and then
    // to function 6.
    tracker.apply(aRelayedStateModificationCommand(5, 6));

    assertThat(tracker.stateOf(5), is(0L));
    assertThat(tracker.stateOf(6), is(1L));
  }
}
