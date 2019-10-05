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

package com.ververica.statefun.flink.core.logger;

import com.ververica.statefun.flink.core.feedback.FeedbackConsumer;
import com.ververica.statefun.flink.core.message.Message;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.SpillingBuffer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;

final class KeyGroupStream {
  private final TypeSerializer<Message> serializer;
  private final SpillingBuffer target;
  private final MemorySegmentPool memoryPool;
  private final DataOutputSerializer output = new DataOutputSerializer(256);

  private long totalSize;
  private int elementCount;

  KeyGroupStream(
      TypeSerializer<Message> serializer,
      IOManager ioManager,
      MemorySegmentPool memorySegmentPool) {
    this.serializer = Objects.requireNonNull(serializer);
    this.memoryPool = Objects.requireNonNull(memorySegmentPool);

    // SpillingBuffer requires at least 1 memory segment to be present at construction, otherwise it
    // fails
    // so we
    memorySegmentPool.ensureAtLeastOneSegmentPresent();
    this.target =
        new SpillingBuffer(ioManager, memorySegmentPool, memorySegmentPool.getSegmentSize());
  }

  static void readFrom(
      DataInputView source, TypeSerializer<Message> serializer, FeedbackConsumer<Message> consumer)
      throws Exception {
    final int elementCount = source.readInt();

    for (int i = 0; i < elementCount; i++) {
      Message envelope = serializer.deserialize(source);
      consumer.processFeedback(envelope);
    }
  }

  private static void copy(@Nonnull DataInputView source, @Nonnull DataOutputView target, long size)
      throws IOException {

    while (size > 0) {
      final int len = (int) Math.min(4 * 1024, size); // read no more then 4k bytes at a time
      target.write(source, len);
      size -= len;
    }
  }

  void append(Message envelope) {
    elementCount++;
    try {
      output.clear();
      serializer.serialize(envelope, output);
      totalSize += output.length();

      target.write(output.getSharedBuffer(), 0, output.length());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  void writeTo(DataOutputView target) throws IOException {
    target.writeInt(elementCount);

    copy(this.target.flip(), target, totalSize);

    for (MemorySegment segment : this.target.close()) {
      memoryPool.release(segment);
    }
  }
}
