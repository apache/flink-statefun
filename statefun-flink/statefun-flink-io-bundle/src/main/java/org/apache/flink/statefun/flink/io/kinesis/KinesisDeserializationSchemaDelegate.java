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

import java.io.IOException;
import java.util.Objects;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.statefun.flink.common.UnimplementedTypeInfo;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressDeserializer;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

final class KinesisDeserializationSchemaDelegate<T> implements KinesisDeserializationSchema<T> {

  private static final long serialVersionUID = 1L;

  private final TypeInformation<T> producedTypeInfo = new UnimplementedTypeInfo<>();
  private final KinesisIngressDeserializer<T> delegate;

  KinesisDeserializationSchemaDelegate(KinesisIngressDeserializer<T> delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public T deserialize(
      byte[] recordValue,
      String partitionKey,
      String seqNum,
      long approxArrivalTimestamp,
      String stream,
      String shardId)
      throws IOException {
    return delegate.deserialize(
        recordValue, partitionKey, seqNum, approxArrivalTimestamp, stream, shardId);
  }

  @Override
  public TypeInformation<T> getProducedType() {
    return producedTypeInfo;
  }
}
