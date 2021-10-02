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

package org.apache.flink.statefun.flink.core.message;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.statefun.flink.core.functions.utils.MessageCounter;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;

/** A {@link RoutableMessage} Builder. */
public final class RoutableMessageBuilder {

  public static RoutableMessageBuilder builder() {
    return new RoutableMessageBuilder();
  }

  @Nullable private Address source;
  private Address target;
  private Object payload;
  private Long priority;
  private Long laxity;
  private Message.MessageType type = Message.MessageType.INGRESS;
  private static MessageCounter counter = new MessageCounter();

  private RoutableMessageBuilder() { }

  public RoutableMessageBuilder withTargetAddress(FunctionType functionType, String id) {
    return withTargetAddress(new Address(functionType, id));
  }

  public RoutableMessageBuilder withTargetAddress(Address target) {
    this.target = Objects.requireNonNull(target);
    return this;
  }

  public RoutableMessageBuilder withSourceAddress(FunctionType functionType, String id) {
    return withSourceAddress(new Address(functionType, id));
  }

  public RoutableMessageBuilder withSourceAddress(@Nullable Address from) {
    this.source = from;
    return this;
  }

  public RoutableMessageBuilder withMessageBody(Object payload) {
    this.payload = Objects.requireNonNull(payload);
    return this;
  }

  public RoutableMessageBuilder withPriority(Long priority) {
    this.priority = Objects.requireNonNull(priority);
    return this;
  }

  public RoutableMessageBuilder withLaxity(Long laxity) {
    this.laxity = Objects.requireNonNull(laxity);
    return this;
  }

  public RoutableMessageBuilder withMessageType(Message.MessageType type){
    this.type = Objects.requireNonNull(type);
    return this;
  }

  public RoutableMessage build() {
    Long id = counter.increment(target);
    return new SdkMessage(source, target, payload, priority, laxity, type, id);
  }
}
