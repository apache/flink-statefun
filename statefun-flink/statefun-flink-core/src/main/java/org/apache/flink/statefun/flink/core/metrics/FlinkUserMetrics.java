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

import static org.apache.flink.statefun.flink.core.metrics.FlinkMetricUtil.wrapFlinkCounterAsSdkCounter;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashMap;
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.statefun.sdk.metrics.Counter;
import org.apache.flink.statefun.sdk.metrics.Metrics;

@Internal
public final class FlinkUserMetrics implements Metrics {
  private final ObjectOpenHashMap<String, Counter> counters = new ObjectOpenHashMap<>();
  private final MetricGroup typeGroup;

  public FlinkUserMetrics(MetricGroup typeGroup) {
    this.typeGroup = Objects.requireNonNull(typeGroup);
  }

  @Override
  public Counter counter(String name) {
    Objects.requireNonNull(name);
    Counter counter = counters.get(name);
    if (counter == null) {
      SimpleCounter internalCounter = typeGroup.counter(name, new SimpleCounter());
      counters.put(name, counter = wrapFlinkCounterAsSdkCounter(internalCounter));
    }
    return counter;
  }
}
