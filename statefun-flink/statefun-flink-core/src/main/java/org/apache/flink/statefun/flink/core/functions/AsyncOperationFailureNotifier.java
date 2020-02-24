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
package org.apache.flink.statefun.flink.core.functions;

import java.util.Map.Entry;
import java.util.Objects;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.statefun.flink.core.message.Message;

final class AsyncOperationFailureNotifier
    implements KeyedStateFunction<String, MapState<Long, Message>> {

  static void fireExpiredAsyncOperations(
      MapStateDescriptor<Long, Message> asyncOperationStateDescriptor,
      Reductions reductions,
      KeyedStateBackend<String> keyedStateBackend)
      throws Exception {

    AsyncOperationFailureNotifier asyncOperationFailureNotifier =
        new AsyncOperationFailureNotifier(reductions);

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

  private boolean enqueued;

  private AsyncOperationFailureNotifier(Reductions reductions) {
    this.reductions = Objects.requireNonNull(reductions);
  }

  @Override
  public void process(String key, MapState<Long, Message> state) throws Exception {
    for (Entry<Long, Message> entry : state.entries()) {
      Long futureId = entry.getKey();
      Message metadataMessage = entry.getValue();
      reductions.enqueueAsyncOperationAfterRestore(futureId, metadataMessage);
      enqueued = true;
    }
  }

  private boolean enqueued() {
    return enqueued;
  }
}
