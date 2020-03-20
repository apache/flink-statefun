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

package org.apache.flink.statefun.flink.io.kinesis.polyglot;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.statefun.flink.io.generated.KinesisEgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.EgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;

public final class GenericKinesisEgressSerializer implements KinesisEgressSerializer<Any> {

  private static final long serialVersionUID = 1L;

  @Override
  public EgressRecord serialize(Any value) {
    final KinesisEgressRecord kinesisEgressRecord = asKinesisEgressRecord(value);
    return EgressRecord.newBuilder()
        .withData(kinesisEgressRecord.getValueBytes().toByteArray())
        .withStream(kinesisEgressRecord.getStream())
        .withPartitionKey(kinesisEgressRecord.getPartitionKey())
        .withExplicitHashKey(kinesisEgressRecord.getExplicitHashKey())
        .build();
  }

  private static KinesisEgressRecord asKinesisEgressRecord(Any message) {
    if (!message.is(KinesisEgressRecord.class)) {
      throw new IllegalStateException(
          "The generic Kinesis egress expects only messages of type "
              + KinesisEgressRecord.class.getName());
    }
    try {
      return message.unpack(KinesisEgressRecord.class);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          "Unable to unpack message as a " + KinesisEgressRecord.class.getName(), e);
    }
  }
}
