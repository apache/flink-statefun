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
package org.apache.flink.statefun.flink.core.functions;

import java.util.OptionalLong;
import java.util.function.Consumer;
import org.apache.flink.statefun.flink.core.message.Message;

interface DelayedMessagesBuffer {

  /** Add a message to be fired at a specific timestamp */
  void add(Message message, long untilTimestamp);

  /** Apply @fn for each delayed message that is meant to be fired at @timestamp. */
  void forEachMessageAt(long timestamp, Consumer<Message> fn);

  /**
   * @param token a message cancellation token to delete.
   * @return an optional timestamp that this message was meant to be fired at. The timestamp will be
   *     present only if this message was the last message registered to fire at that timestamp.
   *     (hence: safe to clear any underlying timer)
   */
  OptionalLong removeMessageByCancellationToken(String token);
}
