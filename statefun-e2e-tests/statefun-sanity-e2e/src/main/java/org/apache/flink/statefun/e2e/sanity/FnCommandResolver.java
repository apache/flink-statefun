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

package org.apache.flink.statefun.e2e.sanity;

import org.apache.flink.statefun.e2e.sanity.generated.VerificationMessages;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.match.MatchBinder;
import org.apache.flink.statefun.sdk.match.StatefulMatchFunction;
import org.apache.flink.statefun.sdk.state.PersistedValue;

/**
 * Simple stateful function that performs actions according to the command received.
 *
 * <ul>
 *   <li>{@link VerificationMessages.Noop} command: does not do anything.
 *   <li>{@link VerificationMessages.Send} command: sends a specified command to another function.
 *   <li>{@link VerificationMessages.Modify} command: increments state value by a specified amount,
 *       and then reflects the new state value to an egress as a {@link
 *       VerificationMessages.StateSnapshot} message.
 * </ul>
 */
public final class FnCommandResolver extends StatefulMatchFunction {

  /** Represents the {@link FunctionType} that this function is bound to. */
  private final int fnTypeIndex;

  FnCommandResolver(int fnTypeIndex) {
    this.fnTypeIndex = fnTypeIndex;
  }

  @Persisted
  private final PersistedValue<Integer> state = PersistedValue.of("state", Integer.class);

  @Override
  public void configure(MatchBinder binder) {
    binder
        .predicate(
            VerificationMessages.Command.class, VerificationMessages.Command::hasNoop, this::noop)
        .predicate(
            VerificationMessages.Command.class, VerificationMessages.Command::hasSend, this::send)
        .predicate(
            VerificationMessages.Command.class,
            VerificationMessages.Command::hasModify,
            this::modify);
  }

  private void send(Context context, VerificationMessages.Command command) {
    for (VerificationMessages.Command send : command.getSend().getCommandToSendList()) {
      Address to = Utils.toSdkAddress(send.getTarget());
      context.send(to, send);
    }
  }

  private void modify(Context context, VerificationMessages.Command command) {
    VerificationMessages.Modify modify = command.getModify();

    final int nextState =
        state.updateAndGet(old -> old == null ? modify.getDelta() : old + modify.getDelta());

    // reflect state changes to egress
    final VerificationMessages.FnAddress self = selfFnAddress(context);
    final VerificationMessages.StateSnapshot result =
        VerificationMessages.StateSnapshot.newBuilder().setFrom(self).setState(nextState).build();

    context.send(Constants.STATE_SNAPSHOT_EGRESS_ID, result);
  }

  @SuppressWarnings("unused")
  private void noop(Context context, Object ignored) {
    // nothing to do
  }

  private VerificationMessages.FnAddress selfFnAddress(Context context) {
    return VerificationMessages.FnAddress.newBuilder()
        .setType(fnTypeIndex)
        .setId(context.self().id())
        .build();
  }
}
