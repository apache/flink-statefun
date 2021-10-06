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
package org.apache.flink.statefun.flink.core.metrics;

import org.apache.flink.metrics.Counter;

/**
 * A simple counter that can never go below zero.
 *
 * <p>This class is used in a non-thread safe manner, so it is important all modifications are
 * checked before updating the count. Otherwise, negative values might be reported.
 */
public class NonNegativeCounter implements Counter {

  private long count;

  @Override
  public void inc() {
    inc(1);
  }

  @Override
  public void inc(long value) {
    count += value;
  }

  @Override
  public void dec() {
    dec(1);
  }

  @Override
  public void dec(long value) {
    if (value > count) {
      count = 0;
    } else {
      count -= value;
    }
  }

  @Override
  public long getCount() {
    return count;
  }
}
