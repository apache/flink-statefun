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

import static org.apache.flink.statefun.e2e.smoke.testutils.Utils.aRelayedStateModificationCommand;
import static org.apache.flink.statefun.e2e.smoke.testutils.Utils.aStateModificationCommand;
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
