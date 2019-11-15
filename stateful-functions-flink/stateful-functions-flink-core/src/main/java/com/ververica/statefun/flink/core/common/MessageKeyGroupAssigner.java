/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.flink.core.common;

import com.ververica.statefun.flink.core.message.Message;
import com.ververica.statefun.sdk.Address;
import java.util.function.ToIntFunction;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

public final class MessageKeyGroupAssigner implements ToIntFunction<Message> {
  private final int maxParallelism;

  public MessageKeyGroupAssigner(int maxParallelism) {
    this.maxParallelism = maxParallelism;
  }

  @Override
  public int applyAsInt(Message message) {
    Address targetAddress = message.target();
    String keyByPart = KeyBy.apply(targetAddress);
    return KeyGroupRangeAssignment.assignToKeyGroup(keyByPart, maxParallelism);
  }
}
