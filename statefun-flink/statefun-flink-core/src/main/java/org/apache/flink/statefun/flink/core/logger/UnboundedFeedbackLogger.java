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
package org.apache.flink.statefun.flink.core.logger;

import static org.apache.flink.util.Preconditions.checkState;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.feedback.FeedbackConsumer;
import org.apache.flink.util.IOUtils;

public final class UnboundedFeedbackLogger<T> implements Closeable {
  private final Supplier<KeyGroupStream<T>> supplier;
  private final ToIntFunction<T> keyGroupAssigner;
  private final Map<Integer, KeyGroupStream<T>> keyGroupStreams;
  private final CheckpointedStreamOperations checkpointedStreamOperations;

  @Nullable private OutputStream keyedStateOutputStream;
  private TypeSerializer<T> serializer;
  private Closeable snapshotLease;

  @Inject
  public UnboundedFeedbackLogger(
      @Label("key-group-supplier") Supplier<KeyGroupStream<T>> supplier,
      @Label("key-group-assigner") ToIntFunction<T> keyGroupAssigner,
      @Label("checkpoint-stream-ops") CheckpointedStreamOperations ops,
      @Label("envelope-serializer") TypeSerializer<T> serializer) {
    this.supplier = Objects.requireNonNull(supplier);
    this.keyGroupAssigner = Objects.requireNonNull(keyGroupAssigner);
    this.serializer = Objects.requireNonNull(serializer);
    this.keyGroupStreams = new TreeMap<>();
    this.checkpointedStreamOperations = Objects.requireNonNull(ops);
  }

  public void startLogging(OutputStream keyedStateCheckpointOutputStream) {
    this.checkpointedStreamOperations.requireKeyedStateCheckpointed(
        keyedStateCheckpointOutputStream);
    this.keyedStateOutputStream = Objects.requireNonNull(keyedStateCheckpointOutputStream);
    this.snapshotLease =
        checkpointedStreamOperations.acquireLease(keyedStateCheckpointOutputStream);
  }

  public void append(T message) {
    if (keyedStateOutputStream == null) {
      //
      // we are not currently logging.
      //
      return;
    }
    KeyGroupStream<T> keyGroup = keyGroupStreamFor(message);
    keyGroup.append(message);
  }

  public void commit() {
    try {
      flushToKeyedStateOutputStream();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      keyGroupStreams.clear();
      IOUtils.closeQuietly(snapshotLease);
      snapshotLease = null;
      keyedStateOutputStream = null;
    }
  }

  private void flushToKeyedStateOutputStream() throws IOException {
    checkState(keyedStateOutputStream != null, "Trying to flush envelopes not in a logging state");

    final DataOutputView target = new DataOutputViewStreamWrapper(keyedStateOutputStream);
    for (Entry<Integer, KeyGroupStream<T>> entry : keyGroupStreams.entrySet()) {
      checkpointedStreamOperations.startNewKeyGroup(keyedStateOutputStream, entry.getKey());

      KeyGroupStream stream = entry.getValue();
      stream.writeTo(target);
    }
  }

  public void replyLoggedEnvelops(InputStream rawKeyedStateInputs, FeedbackConsumer<T> consumer)
      throws Exception {

    DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(rawKeyedStateInputs);
    KeyGroupStream.readFrom(in, serializer, consumer);
  }

  @Nonnull
  private KeyGroupStream<T> keyGroupStreamFor(T target) {
    final int keyGroupId = keyGroupAssigner.applyAsInt(target);
    KeyGroupStream<T> keyGroup = keyGroupStreams.get(keyGroupId);
    if (keyGroup == null) {
      keyGroupStreams.put(keyGroupId, keyGroup = supplier.get());
    }
    return keyGroup;
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(snapshotLease);
    snapshotLease = null;
    keyedStateOutputStream = null;
    keyGroupStreams.clear();
  }
}
