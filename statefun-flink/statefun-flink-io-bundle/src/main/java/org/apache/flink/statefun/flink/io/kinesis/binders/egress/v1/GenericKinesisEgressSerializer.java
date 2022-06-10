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

package org.apache.flink.statefun.flink.io.kinesis.binders.egress.v1;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.statefun.flink.common.types.TypedValueUtil;
import org.apache.flink.statefun.sdk.egress.generated.KinesisEgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.EgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class GenericKinesisEgressSerializer implements KinesisEgressSerializer<TypedValue> {

  private static final long serialVersionUID = 1L;

  @Override
  public EgressRecord serialize(TypedValue value) {
    final KinesisEgressRecord kinesisEgressRecord = asKinesisEgressRecord(value);

    final EgressRecord.Builder builder =
        EgressRecord.newBuilder()
            .withData(kinesisEgressRecord.getValueBytes().toByteArray())
            .withStream(kinesisEgressRecord.getStream())
            .withPartitionKey(kinesisEgressRecord.getPartitionKey());

    final String explicitHashKey = kinesisEgressRecord.getExplicitHashKey();
    if (explicitHashKey != null && !explicitHashKey.isEmpty()) {
      builder.withExplicitHashKey(explicitHashKey);
    }

    return builder.build();
  }

  private static KinesisEgressRecord asKinesisEgressRecord(TypedValue message) {
    if (!TypedValueUtil.isProtobufTypeOf(message, KinesisEgressRecord.getDescriptor())) {
      throw new IllegalStateException(
          "The generic Kinesis egress expects only messages of type "
              + KinesisEgressRecord.class.getName());
    }
    try {
      return KinesisEgressRecord.parseFrom(message.getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          "Unable to unpack message as a " + KinesisEgressRecord.class.getName(), e);
    }
  }
}
