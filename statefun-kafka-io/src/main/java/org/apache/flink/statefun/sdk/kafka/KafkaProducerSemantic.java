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
import java.util.Objects;

public abstract class KafkaProducerSemantic {

  public static KafkaProducerSemantic exactlyOnce(Duration transactionTimeout) {
    return new ExactlyOnce(transactionTimeout);
  }

  public static KafkaProducerSemantic atLeastOnce() {
    return new AtLeastOnce();
  }

  public static KafkaProducerSemantic none() {
    return new NoSemantics();
  }

  public boolean isExactlyOnceSemantic() {
    return getClass() == ExactlyOnce.class;
  }

  public ExactlyOnce asExactlyOnceSemantic() {
    return (ExactlyOnce) this;
  }

  public boolean isAtLeastOnceSemantic() {
    return getClass() == AtLeastOnce.class;
  }

  public AtLeastOnce asAtLeastOnceSemantic() {
    return (AtLeastOnce) this;
  }

  public boolean isNoSemantic() {
    return getClass() == NoSemantics.class;
  }

  public NoSemantics asNoSemantic() {
    return (NoSemantics) this;
  }

  public static class ExactlyOnce extends KafkaProducerSemantic {
    private final Duration transactionTimeout;

    private ExactlyOnce(Duration transactionTimeout) {
      if (transactionTimeout == Duration.ZERO) {
        throw new IllegalArgumentException(
            "Transaction timeout durations must be larger than 0 when using exactly-once producer semantics.");
      }
      this.transactionTimeout = Objects.requireNonNull(transactionTimeout);
    }

    public Duration transactionTimeout() {
      return transactionTimeout;
    }
  }

  public static class AtLeastOnce extends KafkaProducerSemantic {
    private AtLeastOnce() {}
  }

  public static class NoSemantics extends KafkaProducerSemantic {
    private NoSemantics() {}
  }
}
