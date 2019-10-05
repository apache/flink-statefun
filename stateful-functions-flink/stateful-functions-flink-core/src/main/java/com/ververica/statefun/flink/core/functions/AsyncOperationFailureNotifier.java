/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.flink.core.functions;

import com.ververica.statefun.flink.core.message.Message;
import java.util.Map.Entry;
import java.util.Objects;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;

final class AsyncOperationFailureNotifier
    implements KeyedStateFunction<String, MapState<Long, Message>> {

  static void fireExpiredAsyncOperations(
      MapStateDescriptor<Long, Message> asyncOperationStateDescriptor,
      Reductions reductions,
      MapState<Long, Message> asyncOperationState,
      KeyedStateBackend<String> keyedStateBackend)
      throws Exception {

    AsyncOperationFailureNotifier asyncOperationFailureNotifier =
        new AsyncOperationFailureNotifier(reductions, asyncOperationState);

    keyedStateBackend.applyToAllKeys(
        VoidNamespace.get(),
        VoidNamespaceSerializer.INSTANCE,
        asyncOperationStateDescriptor,
        asyncOperationFailureNotifier);

    if (asyncOperationFailureNotifier.enqueued()) {
      reductions.processEnvelopes();
    }
  }

  private final Reductions reductions;
  private final MapState<Long, Message> asyncOperationState;

  private boolean enqueued;

  private AsyncOperationFailureNotifier(
      Reductions reductions, MapState<Long, Message> asyncOperationState) {
    this.reductions = Objects.requireNonNull(reductions);
    this.asyncOperationState = Objects.requireNonNull(asyncOperationState);
  }

  @Override
  public void process(String key, MapState<Long, Message> state) throws Exception {
    for (Entry<Long, Message> entry : state.entries()) {
      Long futureId = entry.getKey();
      Message metadataMessage = entry.getValue();
      Message adaptor = new AsyncMessageDecorator(asyncOperationState, futureId, metadataMessage);
      reductions.enqueue(adaptor);
      enqueued = true;
    }
  }

  private boolean enqueued() {
    return enqueued;
  }
}
