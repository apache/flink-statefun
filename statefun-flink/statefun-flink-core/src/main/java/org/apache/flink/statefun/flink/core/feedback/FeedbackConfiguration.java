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
package org.apache.flink.statefun.flink.core.feedback;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

public final class FeedbackConfiguration {
  private FeedbackConfiguration() {}

  public static final ConfigOption<MemorySize> TOTAL_MEMORY_USED_FOR_FEEDBACK_CHECKPOINTING =
      ConfigOptions.key("statefun.feedback.memory.size")
          .memoryType()
          .defaultValue(MemorySize.ofMebiBytes(32))
          .withDescription(
              "The number of bytes to use for in memory buffering of the feedback channel, before spilling to disk");
}
