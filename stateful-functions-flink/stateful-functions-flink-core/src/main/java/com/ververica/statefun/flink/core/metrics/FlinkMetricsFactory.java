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

package com.ververica.statefun.flink.core.metrics;

import com.ververica.statefun.sdk.FunctionType;
import java.util.Objects;
import org.apache.flink.metrics.MetricGroup;

public class FlinkMetricsFactory implements MetricsFactory {

  private final MetricGroup metricGroup;

  public FlinkMetricsFactory(MetricGroup metricGroup) {
    this.metricGroup = Objects.requireNonNull(metricGroup);
  }

  @Override
  public FunctionTypeMetrics forType(FunctionType functionType) {
    MetricGroup namespace = metricGroup.addGroup(functionType.namespace());
    MetricGroup typeGroup = namespace.addGroup(functionType.name());
    return new FlinkFunctionTypeMetrics(typeGroup);
  }
}
