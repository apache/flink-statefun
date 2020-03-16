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
package org.apache.flink.statefun.sdk.kinesis.egress;

import java.util.Objects;
import javax.annotation.Nullable;

/** A record to be written to AWS Kinesis. */
public final class EgressRecord {

  private final byte[] data;
  private final String stream;
  private final String partitionKey;
  @Nullable private final String explicitHashKey;

  /** @return A builder for a {@link EgressRecord}. */
  public static Builder newBuilder() {
    return new Builder();
  }

  private EgressRecord(
      byte[] data, String stream, String partitionKey, @Nullable String explicitHashKey) {
    this.data = Objects.requireNonNull(data, "data bytes");
    this.stream = Objects.requireNonNull(stream, "target stream");
    this.partitionKey = Objects.requireNonNull(partitionKey, "partition key");
    this.explicitHashKey = explicitHashKey;
  }

  /** @return data bytes to write */
  public byte[] getData() {
    return data;
  }

  /** @return target AWS Kinesis stream to write to. */
  public String getStream() {
    return stream;
  }

  /** @return partition key to use when writing the record to AWS Kinesis. */
  public String getPartitionKey() {
    return partitionKey;
  }

  /** @return explicit hash key to use when writing the record to AWS Kinesis. */
  @Nullable
  public String getExplicitHashKey() {
    return explicitHashKey;
  }

  /** Builder for {@link EgressRecord}. */
  public static final class Builder {
    private byte[] data;
    private String stream;
    private String partitionKey;
    private String explicitHashKey;

    private Builder() {}

    public Builder withData(byte[] data) {
      this.data = data;
      return this;
    }

    public Builder withStream(String stream) {
      this.stream = stream;
      return this;
    }

    public Builder withPartitionKey(String partitionKey) {
      this.partitionKey = partitionKey;
      return this;
    }

    public Builder withExplicitHashKey(String explicitHashKey) {
      this.explicitHashKey = explicitHashKey;
      return this;
    }

    public EgressRecord build() {
      return new EgressRecord(data, stream, partitionKey, explicitHashKey);
    }
  }
}
