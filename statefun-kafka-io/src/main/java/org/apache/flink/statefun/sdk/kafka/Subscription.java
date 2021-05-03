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
package org.apache.flink.statefun.sdk.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Represents the ingresses subscription to the Kafka cluster; which may either be a list of topics
 * or a pattern but not both.
 */
public final class Subscription {

  @Nullable private List<String> topics;

  @Nullable private PatternSubscription patternSubscription;

  void withTopic(String topic) {
    Objects.requireNonNull(topic);
    if (patternSubscription != null) {
      throw new IllegalStateException(
          "Cannot subscribe to topic, this ingress has already been configured for pattern based subscription");
    }

    if (topics == null) {
      topics = new ArrayList<>();
    }

    topics.add(topic);
  }

  void withTopics(List<String> topics) {
    Objects.requireNonNull(topics);
    if (patternSubscription != null) {
      throw new IllegalStateException(
          "Cannot subscribe to topics, this ingress has already been configured for pattern based subscription");
    }

    if (this.topics == null) {
      this.topics = new ArrayList<>();
    }

    this.topics.addAll(topics);
  }

  void withPattern(Pattern topicPattern, Duration discoveryInterval) {
    if (topics != null) {
      throw new IllegalStateException(
          "Cannot subscribe to topic pattern, this ingress has already been configured for one or more concrete topics");
    }

    if (patternSubscription != null) {
      throw new IllegalStateException("One ingress cannot subscribe to multiple patterns");
    }

    patternSubscription = new PatternSubscription(topicPattern, discoveryInterval);
  }

  public Optional<List<String>> getTopicSubscription() {
    return Optional.ofNullable(topics);
  }

  public Optional<PatternSubscription> getPatternSubscription() {
    return Optional.ofNullable(patternSubscription);
  }

  public static class PatternSubscription {
    private final Pattern topicPattern;
    private final Duration discoveryInterval;

    public PatternSubscription(Pattern topicPattern, Duration discoveryInterval) {
      this.topicPattern = Objects.requireNonNull(topicPattern);
      this.discoveryInterval = Objects.requireNonNull(discoveryInterval);

      if (discoveryInterval.isZero()) {
        throw new IllegalArgumentException("Discovery interval durations must be larger than 0.");
      }
    }

    public Pattern getTopicPattern() {
      return topicPattern;
    }

    public Duration getDiscoveryInterval() {
      return discoveryInterval;
    }
  }
}
