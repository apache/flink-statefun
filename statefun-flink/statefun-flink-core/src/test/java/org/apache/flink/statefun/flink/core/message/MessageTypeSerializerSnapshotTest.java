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
package org.apache.flink.statefun.flink.core.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MessageTypeSerializerSnapshotTest {

  private static final String serializerClassName = "com.domain.Serializer";

  private static class SnapshotData {
    public int version;
    public byte[] bytes;
  }

  private static interface SnapshotDataProvider {
    SnapshotData provide(MessageFactoryKey messageFactoryKey) throws IOException;
  }

  private final MessageFactoryKey messageFactoryKey;
  private final SnapshotDataProvider snapshotDataProvider;

  public MessageTypeSerializerSnapshotTest(
      MessageFactoryKey messageFactoryKey, SnapshotDataProvider snapshotDataProvider) {
    this.messageFactoryKey = messageFactoryKey;
    this.snapshotDataProvider = snapshotDataProvider;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<? extends Object[]> data() throws IOException {

    MessageFactoryKey kryoFactoryKey =
        MessageFactoryKey.forType(MessageFactoryType.WITH_KRYO_PAYLOADS, null);
    MessageFactoryKey customFactoryKey =
        MessageFactoryKey.forType(MessageFactoryType.WITH_CUSTOM_PAYLOADS, serializerClassName);

    // generates snapshot data for V1, without customPayloadSerializerClassName
    SnapshotDataProvider snapshotDataProviderV1 =
        messageFactoryKey -> {
          try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            DataOutputView dataOutputView = new DataOutputViewStreamWrapper(bos);
            dataOutputView.writeUTF(messageFactoryKey.getType().name());
            return new SnapshotData() {
              {
                version = 1;
                bytes = bos.toByteArray();
              }
            };
          }
        };

    // generates snapshot data for V2, the current version
    SnapshotDataProvider snapshotDataProviderV2 =
        messageFactoryKey -> {
          MessageTypeSerializer.Snapshot snapshot =
              new MessageTypeSerializer.Snapshot(messageFactoryKey);
          try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            DataOutputView dataOutputView = new DataOutputViewStreamWrapper(bos);
            snapshot.writeSnapshot(dataOutputView);
            return new SnapshotData() {
              {
                version = 2;
                bytes = bos.toByteArray();
              }
            };
          }
        };

    return Arrays.asList(
        new Object[] {kryoFactoryKey, snapshotDataProviderV1},
        new Object[] {kryoFactoryKey, snapshotDataProviderV2},
        new Object[] {customFactoryKey, snapshotDataProviderV2});
  }

  @Test
  public void roundTrip() throws IOException {

    SnapshotData snapshotData = this.snapshotDataProvider.provide(this.messageFactoryKey);
    MessageTypeSerializer.Snapshot snapshot =
        new MessageTypeSerializer.Snapshot(this.messageFactoryKey);
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    try (ByteArrayInputStream bis = new ByteArrayInputStream(snapshotData.bytes)) {
      DataInputView dataInputView = new DataInputViewStreamWrapper(bis);
      snapshot.readSnapshot(snapshotData.version, dataInputView, classLoader);
    }

    // make sure the deserialized state matches what was used to serialize
    assert (snapshot.getMessageFactoryKey().equals(this.messageFactoryKey));
  }
}
