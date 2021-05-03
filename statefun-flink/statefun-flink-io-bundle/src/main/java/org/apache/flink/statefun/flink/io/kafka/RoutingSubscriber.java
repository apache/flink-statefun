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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.sdk.kafka.Subscription;

final class RoutingSubscriber {

  private final Subscription subscription;

  private final RoutingConfigAssigner assigner;

  RoutingSubscriber(Subscription subscription, RoutingConfigAssigner assigner) {
    this.subscription = subscription;
    this.assigner = assigner;
  }

  Subscription getSubscription() {
    return subscription;
  }

  RoutingConfigAssigner getAssigner() {
    return assigner;
  }

  static RoutingSubscriber fromConfigMap(Map<String, RoutingConfig> routingConfigs) {
    return new RoutingSubscriber(
        new Subscription().withTopics(new ArrayList<>(routingConfigs.keySet())),
        RoutingConfigAssigner.fromTopicMap(routingConfigs));
  }

  static RoutingSubscriber fromPattern(
      Pattern pattern, Duration discoveryInterval, RoutingConfig routingConfig) {
    return new RoutingSubscriber(
        new Subscription().withPattern(pattern, discoveryInterval),
        RoutingConfigAssigner.fromStaticConfig(routingConfig));
  }
}
