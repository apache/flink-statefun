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
package org.apache.flink.statefun.flink.io.kafka;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;

public abstract class RoutingConfigAssigner implements Serializable {

  private static final long serialVersionUID = 1L;

  public static RoutingConfigAssigner fromTopicMap(Map<String, RoutingConfig> routingConfigs) {
    return new SpecificTopicAssigner(routingConfigs);
  }

  public static RoutingConfigAssigner fromStaticConfig(RoutingConfig config) {
    return new StaticTopicAssigner(config);
  }

  abstract RoutingConfig get(String topic);

  private static class SpecificTopicAssigner extends RoutingConfigAssigner {

    private final Map<String, RoutingConfig> routingConfigs;

    private SpecificTopicAssigner(Map<String, RoutingConfig> routingConfigs) {
      if (routingConfigs == null || routingConfigs.isEmpty()) {
        throw new IllegalArgumentException(
            "Routing config for routable Kafka ingress cannot be empty.");
      }
      this.routingConfigs = routingConfigs;
    }

    @Override
    RoutingConfig get(String topic) {
      return routingConfigs.get(topic);
    }
  }

  private static class StaticTopicAssigner extends RoutingConfigAssigner {

    private final RoutingConfig config;

    private StaticTopicAssigner(RoutingConfig config) {
      this.config = Objects.requireNonNull(config);
    }

    @Override
    RoutingConfig get(String topic) {
      return config;
    }
  }
}
