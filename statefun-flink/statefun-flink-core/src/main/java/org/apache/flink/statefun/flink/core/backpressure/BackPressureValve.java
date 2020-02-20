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

package org.apache.flink.statefun.flink.core.backpressure;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.Address;

public interface BackPressureValve {

  /**
   * Indicates rather a back pressure is needed.
   *
   * @return true if a back pressure should be applied.
   */
  boolean shouldBackPressure();

  /**
   * Notifies the back pressure mechanism that a async operation was registered via {@link
   * org.apache.flink.statefun.sdk.Context#registerAsyncOperation(Object, CompletableFuture)}.
   */
  void notifyAsyncOperationRegistered();

  /**
   * Notifies when a async operation, registered by @owningAddress was completed.
   *
   * @param owningAddress the owner of the completed async operation.
   */
  void notifyAsyncOperationCompleted(Address owningAddress);

  /**
   * Requests to stop processing any further input for that address, as long as there is an
   * uncompleted async operation (owned by @address).
   *
   * @param address the address
   */
  void blockAddress(Address address);
}
