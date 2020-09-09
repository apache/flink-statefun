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

package org.apache.flink.statefun.flink.core.reqreply;

import java.util.Objects;
import org.apache.flink.statefun.sdk.Address;

/**
 * ToFunctionRequestSummary - represents a summary of that request, it is indented to be used as an
 * additional context for logging.
 */
public final class ToFunctionRequestSummary {
  private final Address address;
  private final int batchSize;
  private final int totalSizeInBytes;
  private final int numberOfStates;

  public ToFunctionRequestSummary(
      Address address, int totalSizeInBytes, int numberOfStates, int batchSize) {
    this.address = Objects.requireNonNull(address);
    this.totalSizeInBytes = totalSizeInBytes;
    this.numberOfStates = numberOfStates;
    this.batchSize = batchSize;
  }

  @Override
  public String toString() {
    return "ToFunctionRequestSummary("
        + "address="
        + address
        + ", batchSize="
        + batchSize
        + ", totalSizeInBytes="
        + totalSizeInBytes
        + ", numberOfStates="
        + numberOfStates
        + ')';
  }
}
