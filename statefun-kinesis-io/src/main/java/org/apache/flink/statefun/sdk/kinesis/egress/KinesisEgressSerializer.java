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

/**
 * Defines how to serialize values of type {@code T} into {@link EgressRecord}s to be written to AWS
 * Kinesis.
 *
 * @param <T> the type of values being written.
 */
public interface KinesisEgressSerializer<T> extends Serializable {

  /**
   * Serialize an output value into a {@link EgressRecord} to be written to AWS Kinesis.
   *
   * @param value the output value to write.
   * @return a {@link EgressRecord} to be written to AWS Kinesis.
   */
  EgressRecord serialize(T value);
}
