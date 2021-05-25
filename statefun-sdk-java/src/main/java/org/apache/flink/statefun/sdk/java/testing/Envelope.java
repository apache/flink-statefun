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
import org.apache.flink.statefun.sdk.java.message.Message;

/**
 * A utility class that wraps a {@link Message} and the {@link Duration} after which it was sent by
 * a {@link org.apache.flink.statefun.sdk.java.StatefulFunction}. It is used by the {@link
 * TestContext}.
 */
public class Envelope {

  private final Duration duration;
  private final Message message;

  public Envelope(Duration duration, Message message) {
    this.duration = duration;
    this.message = message;
  }

  public Duration getDuration() {
    return duration;
  }

  public Message getMessage() {
    return message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Envelope that = (Envelope) o;
    return Objects.equals(duration, that.duration) && Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(duration, message);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("SentMessage{");
    sb.append("duration=").append(duration);
    sb.append(", message=").append(message);
    sb.append('}');
    return sb.toString();
  }
}
