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
package org.apache.flink.statefun.sdk.kinesis.ingress;

import java.io.Serializable;

/**
 * Describes how to turn AWS Kinesis record data bytes into data types that are processed by the
 * system.
 *
 * @param <T> The type created by the ingress deserializer.
 */
public interface KinesisIngressDeserializer<T> extends Serializable {

  /**
   * Deserializes an AWS Kinesis record's data bytes.
   *
   * @param recordValue the record's data bytes.
   * @param partitionKey the record's partition key.
   * @param seqNum the record's position, i.e. sequence number, in the shard.
   * @param approxArrivalTimestamp the record's approximate arrival timestamp (also known as
   *     ingestion timestamp) at Kinesis.
   * @param stream the stream the record was consumed from.
   * @param shardId the id of the shard the record was consumed from.
   */
  T deserialize(
      byte[] recordValue,
      String partitionKey,
      String seqNum,
      long approxArrivalTimestamp,
      String stream,
      String shardId);
}
