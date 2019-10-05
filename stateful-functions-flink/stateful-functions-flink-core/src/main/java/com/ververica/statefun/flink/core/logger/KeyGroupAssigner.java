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

package com.ververica.statefun.flink.core.logger;

import com.ververica.statefun.flink.core.common.KeyBy;
import com.ververica.statefun.flink.core.di.Inject;
import com.ververica.statefun.flink.core.di.Label;
import com.ververica.statefun.sdk.Address;
import java.util.function.ToIntFunction;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

final class KeyGroupAssigner implements ToIntFunction<Address> {
  private final int maxParallelism;

  @Inject
  KeyGroupAssigner(@Label("max-parallelism") int maxParallelism) {
    this.maxParallelism = maxParallelism;
  }

  @Override
  public int applyAsInt(Address target) {
    return KeyGroupRangeAssignment.assignToKeyGroup(KeyBy.apply(target), maxParallelism);
  }
}
