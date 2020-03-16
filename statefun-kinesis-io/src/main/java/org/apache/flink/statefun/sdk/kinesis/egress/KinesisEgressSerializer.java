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

import java.io.Serializable;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;

/**
 * Defines how to serialize values of type {@code T} into AWS Kinesis records.
 *
 * @param <T> the type of values being serialized.
 */
public interface KinesisEgressSerializer<T> extends Serializable {

  /**
   * Serializes a record into data bytes.
   *
   * @param record the record being written.
   */
  ByteBuffer serialize(T record);

  /**
   * The target stream to write to for a given record.
   *
   * @param record the record being written.
   */
  String targetStream(T record);

  /**
   * The partition key for a given record.
   *
   * @param record the record being written.
   */
  String partitionKey(T record);

  /**
   * The explicit hash key for a given record. By default, the explicit hash key is {@code null}.
   *
   * @param record the record being written.
   */
  @Nullable
  default String explicitHashKey(T record) {
    return null;
  }
}
