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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.flink.statefun.sdk.kinesis.egress.EgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;
import org.junit.Test;

public class CachingPartitionerSerializerDelegateTest {

  private static final String TEST_INPUT = "input";
  private static final String TEST_STREAM = "stream";
  private static final String TEST_PARTITION_KEY = "partition-key";
  private static final String TEST_EXPLICIT_HASH_KEY = "explicit-hash-key";

  @Test
  public void noDuplicateSerialization() {
    final CachingPartitionerSerializerDelegate<String> cachingDelegate =
        new CachingPartitionerSerializerDelegate<>(new DuplicateSerializationDetectingSerializer());

    cachingDelegate.serialize(TEST_INPUT);

    // these throw if the wrapped serializer is used multiple times
    cachingDelegate.getTargetStream(TEST_INPUT);
    cachingDelegate.getPartitionId(TEST_INPUT);
    cachingDelegate.getExplicitHashKey(TEST_INPUT);
  }

  @Test
  public void serialize() {
    final CachingPartitionerSerializerDelegate<String> cachingDelegate =
        new CachingPartitionerSerializerDelegate<>(new DuplicateSerializationDetectingSerializer());

    assertThat(
        cachingDelegate.serialize(TEST_INPUT),
        is(ByteBuffer.wrap(TEST_INPUT.getBytes(StandardCharsets.UTF_8))));
  }

  @Test
  public void targetStream() {
    final CachingPartitionerSerializerDelegate<String> cachingDelegate =
        new CachingPartitionerSerializerDelegate<>(new DuplicateSerializationDetectingSerializer());

    assertThat(cachingDelegate.getTargetStream(TEST_INPUT), is(TEST_STREAM));
  }

  @Test
  public void partitionId() {
    final CachingPartitionerSerializerDelegate<String> cachingDelegate =
        new CachingPartitionerSerializerDelegate<>(new DuplicateSerializationDetectingSerializer());

    assertThat(cachingDelegate.getPartitionId(TEST_INPUT), is(TEST_PARTITION_KEY));
  }

  @Test
  public void explicitHashKey() {
    final CachingPartitionerSerializerDelegate<String> cachingDelegate =
        new CachingPartitionerSerializerDelegate<>(new DuplicateSerializationDetectingSerializer());

    assertThat(cachingDelegate.getExplicitHashKey(TEST_INPUT), is(TEST_EXPLICIT_HASH_KEY));
  }

  private static class DuplicateSerializationDetectingSerializer
      implements KinesisEgressSerializer<String> {

    private boolean isInvoked;

    @Override
    public EgressRecord serialize(String value) {
      if (isInvoked) {
        fail("Duplicate serialization detected.");
      }
      isInvoked = true;
      return EgressRecord.newBuilder()
          .withData(value.getBytes(StandardCharsets.UTF_8))
          .withStream(TEST_STREAM)
          .withPartitionKey(TEST_PARTITION_KEY)
          .withExplicitHashKey(TEST_EXPLICIT_HASH_KEY)
          .build();
    }
  }
}
