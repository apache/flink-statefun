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

package org.apache.flink.statefun.flink.io.kinesis;

import java.nio.ByteBuffer;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.statefun.sdk.kinesis.egress.EgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.KinesisPartitioner;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema;

/**
 * An implementation of a {@link KinesisPartitioner} and {@link KinesisSerializationSchema}, that
 * delegates partitioning and serialization to a wrapped {@link KinesisEgressSerializer}, while also
 * caching already processed element objects to avoid duplicate serialization.
 *
 * <p>To avoid duplicate serialization, a shared instance of this is used as both the partitioner
 * and the serialization schema within a single subtask of a {@link FlinkKinesisProducer}.
 *
 * <p>Note that this class is not thread-safe, and should not be accessed concurrently.
 *
 * @param <T>
 */
@NotThreadSafe
final class CachingPartitionerSerializerDelegate<T> extends KinesisPartitioner<T>
    implements KinesisSerializationSchema<T> {

  private static final long serialVersionUID = 1L;

  private final KinesisEgressSerializer<T> delegate;

  private transient T lastProcessedElement;
  private transient EgressRecord lastSerializedRecord;

  CachingPartitionerSerializerDelegate(KinesisEgressSerializer<T> delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public ByteBuffer serialize(T element) {
    return ByteBuffer.wrap(getLastOrCreateNewSerializedRecord(element).getData());
  }

  @Override
  public String getTargetStream(T element) {
    return getLastOrCreateNewSerializedRecord(element).getStream();
  }

  @Override
  public String getPartitionId(T element) {
    return getLastOrCreateNewSerializedRecord(element).getPartitionKey();
  }

  @Override
  public String getExplicitHashKey(T element) {
    return getLastOrCreateNewSerializedRecord(element).getExplicitHashKey();
  }

  private EgressRecord getLastOrCreateNewSerializedRecord(T element) {
    if (element == lastProcessedElement) {
      return lastSerializedRecord;
    }
    lastProcessedElement = element;
    lastSerializedRecord = delegate.serialize(element);
    return lastSerializedRecord;
  }
}
