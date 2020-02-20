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

import org.apache.flink.annotation.Internal;

@Internal
public interface AsyncWaiter {

  /**
   * Signals the runtime to stop invoking the currently executing function with new input until at
   * least one {@link org.apache.flink.statefun.sdk.AsyncOperationResult} belonging to this function
   * would be delivered.
   */
  void awaitAsyncOperationComplete();
}
