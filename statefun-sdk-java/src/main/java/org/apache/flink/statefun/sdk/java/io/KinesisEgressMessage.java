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
package org.apache.flink.statefun.sdk.java.io;

import com.google.protobuf.ByteString;
import java.util.Objects;
import org.apache.flink.statefun.sdk.egress.generated.KinesisEgressRecord;
import org.apache.flink.statefun.sdk.java.ApiExtension;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageWrapper;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.TypeSerializer;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class KinesisEgressMessage {

  public static Builder forEgress(TypeName targetEgressId) {
    Objects.requireNonNull(targetEgressId);
    return new Builder(targetEgressId);
  }

  public static final class Builder {
    private static final TypeName KINESIS_PRODUCER_RECORD_TYPENAME =
        TypeName.typeNameOf(
            "type.googleapis.com", KinesisEgressRecord.getDescriptor().getFullName());

    private final TypeName targetEgressId;
    private ByteString targetStreamBytes;
    private ByteString partitionKeyBytes;
    private ByteString valueBytes;
    private ByteString explicitHashKey;

    private Builder(TypeName targetEgressId) {
      this.targetEgressId = targetEgressId;
    }

    public Builder withStream(String stream) {
      Objects.requireNonNull(stream);
      this.targetStreamBytes = ByteString.copyFromUtf8(stream);
      return this;
    }

    public Builder withStream(Slice stream) {
      this.targetStreamBytes = SliceProtobufUtil.asByteString(stream);
      return this;
    }

    public Builder withUtf8PartitionKey(String key) {
      Objects.requireNonNull(key);
      this.partitionKeyBytes = ByteString.copyFromUtf8(key);
      return this;
    }

    public Builder withPartitionKey(byte[] key) {
      Objects.requireNonNull(key);
      this.partitionKeyBytes = ByteString.copyFrom(key);
      return this;
    }

    public Builder withPartitionKey(Slice key) {
      Objects.requireNonNull(key);
      this.partitionKeyBytes = SliceProtobufUtil.asByteString(key);
      return this;
    }

    public Builder withUtf8Value(String value) {
      Objects.requireNonNull(value);
      this.valueBytes = ByteString.copyFromUtf8(value);
      return this;
    }

    public Builder withValue(byte[] value) {
      Objects.requireNonNull(value);
      this.valueBytes = ByteString.copyFrom(value);
      return this;
    }

    public Builder withValue(Slice value) {
      Objects.requireNonNull(value);
      this.valueBytes = SliceProtobufUtil.asByteString(value);
      return this;
    }

    public <T> Builder withValue(Type<T> type, T value) {
      TypeSerializer<T> serializer = type.typeSerializer();
      return withValue(serializer.serialize(value));
    }

    public Builder withUtf8ExplicitHashKey(String value) {
      Objects.requireNonNull(value);
      this.explicitHashKey = ByteString.copyFromUtf8(value);
      return this;
    }

    public Builder withUtf8ExplicitHashKey(Slice utf8Slice) {
      Objects.requireNonNull(utf8Slice);
      this.explicitHashKey = SliceProtobufUtil.asByteString(utf8Slice);
      return this;
    }

    public EgressMessage build() {
      KinesisEgressRecord.Builder builder = KinesisEgressRecord.newBuilder();
      if (targetStreamBytes == null) {
        throw new IllegalStateException("Missing destination Kinesis stream");
      }
      builder.setStreamBytes(targetStreamBytes);
      if (partitionKeyBytes == null) {
        throw new IllegalStateException("Missing partition key");
      }
      builder.setPartitionKeyBytes(partitionKeyBytes);
      if (valueBytes == null) {
        throw new IllegalStateException("Missing value");
      }
      builder.setValueBytes(valueBytes);
      if (explicitHashKey != null) {
        builder.setExplicitHashKeyBytes(explicitHashKey);
      }

      TypedValue typedValue =
          TypedValue.newBuilder()
              .setTypenameBytes(ApiExtension.typeNameByteString(KINESIS_PRODUCER_RECORD_TYPENAME))
              .setValue(builder.build().toByteString())
              .build();

      return new EgressMessageWrapper(targetEgressId, typedValue);
    }
  }
}
