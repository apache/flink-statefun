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
package org.apache.flink.statefun.sdk.java.testing;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;

public final class SideEffects {
  private SideEffects() {}

  public static SendSideEffect sentMessage(Message message) {
    return new SendSideEffect(message);
  }

  public static SendAfterSideEffect sentAfter(Duration duration, Message message) {
    return new SendAfterSideEffect(duration, message);
  }

  public static EgressSideEffect sentEgress(EgressMessage message) {
    return new EgressSideEffect(message);
  }

  /**
   * A utility class that wraps a {@link EgressMessage} thats was sent by a {@link
   * org.apache.flink.statefun.sdk.java.StatefulFunction}. It is used by the {@link TestContext}.
   */
  public static final class EgressSideEffect {

    private final EgressMessage message;

    public EgressSideEffect(EgressMessage message) {
      this.message = Objects.requireNonNull(message);
    }

    public EgressMessage message() {
      return message;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      EgressSideEffect envelope = (EgressSideEffect) o;
      return Objects.equals(message, envelope.message);
    }

    @Override
    public int hashCode() {
      return message.hashCode();
    }

    @Override
    public String toString() {
      return "EgressEnvelope{" + "message=" + message + '}';
    }
  }

  /**
   * A utility class that wraps a {@link Message} and the {@link Duration} after which it was sent
   * by a {@link org.apache.flink.statefun.sdk.java.StatefulFunction}. It is used by the {@link
   * TestContext}.
   */
  public static final class SendAfterSideEffect {

    private final Duration duration;
    private final Message message;
    private final String cancellationToken;

    public SendAfterSideEffect(Duration duration, Message message) {
      this.duration = Objects.requireNonNull(duration);
      this.message = Objects.requireNonNull(message);
      this.cancellationToken = null;
    }

    public SendAfterSideEffect(Duration duration, Message message, String cancellationToken) {
      this.duration = Objects.requireNonNull(duration);
      this.message = Objects.requireNonNull(message);
      this.cancellationToken = Objects.requireNonNull(cancellationToken);
    }

    public Duration duration() {
      return duration;
    }

    public Message message() {
      return message;
    }

    public Optional<String> cancellationToken() {
      return Optional.ofNullable(cancellationToken);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SendAfterSideEffect that = (SendAfterSideEffect) o;
      return Objects.equals(duration, that.duration)
          && Objects.equals(message, that.message)
          && Objects.equals(cancellationToken, that.cancellationToken);
    }

    @Override
    public int hashCode() {
      return Objects.hash(duration, message, cancellationToken);
    }

    @Override
    public String toString() {
      return "DelayedEnvelope{" + "duration=" + duration + ", message=" + message + '}';
    }
  }

  /**
   * A utility class that wraps a {@link Message} that was sent by a {@link
   * org.apache.flink.statefun.sdk.java.StatefulFunction}. It is used by the {@link TestContext}.
   */
  public static final class SendSideEffect {

    private final Message message;

    public SendSideEffect(Message message) {
      this.message = Objects.requireNonNull(message);
    }

    public Message message() {
      return message;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SendSideEffect sendSideEffect = (SendSideEffect) o;
      return Objects.equals(message, sendSideEffect.message);
    }

    @Override
    public int hashCode() {
      return message.hashCode();
    }

    @Override
    public String toString() {
      return "Envelope{" + "message=" + message + '}';
    }
  }
}
