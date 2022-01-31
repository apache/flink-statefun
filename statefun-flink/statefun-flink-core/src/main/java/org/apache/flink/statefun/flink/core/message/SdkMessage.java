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

import java.io.IOException;
import java.util.Objects;
import java.util.OptionalLong;
import javax.annotation.Nullable;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.statefun.flink.core.generated.Envelope;
import org.apache.flink.statefun.flink.core.generated.Envelope.Builder;
import org.apache.flink.statefun.flink.core.generated.EnvelopeAddress;
import org.apache.flink.statefun.sdk.Address;

final class SdkMessage extends Message {

  @Nullable private final Address source;

  private Address target;

  private PriorityObject priority;

  private Object payload;

  @Nullable private Envelope cachedEnvelope;

  private MessageType type;

  private Long id;

  private Address lessor;

  private Boolean requiresACK;

  SdkMessage(@Nullable Address source, Address target, Object payload, long priority, long laxity, Long id) {
    this.source = source;
    this.target = Objects.requireNonNull(target);
    this.payload = Objects.requireNonNull(payload);
    this.priority = new PriorityObject(priority, laxity);
    this.type = MessageType.REQUEST;
    this.id = id;
    this.requiresACK = false;
  }

  SdkMessage(@Nullable Address source, Address target, Object payload, long priority, long laxity, MessageType messageType, Long id) {
    this.source = source;
    this.target = Objects.requireNonNull(target);
    this.payload = Objects.requireNonNull(payload);
    this.priority = new PriorityObject(priority, laxity);
    this.type = messageType;
    this.id = id;
    this.requiresACK = false;
  }

  SdkMessage(@Nullable Address source, Address target, Object payload, long priority, Long id) {
    this.source = source;
    this.target = Objects.requireNonNull(target);
    this.payload = Objects.requireNonNull(payload);
    this.priority = new PriorityObject(priority, 0L);
    this.type = MessageType.REQUEST;
    this.id = id;
    this.requiresACK = false;
  }

  SdkMessage(@Nullable Address source, Address target, Object payload, long priority, MessageType messageType, Long id) {
    this.source = source;
    this.target = Objects.requireNonNull(target);
    this.payload = Objects.requireNonNull(payload);
    this.priority = new PriorityObject(priority, 0L);
    this.type = messageType;
    this.id = id;
    this.requiresACK = false;
  }


  @Override
  @Nullable
  public Address source() {
    return source;
  }

  @Override
  public Address target() {
    return target;
  }

  @Override
  public Object payload(MessageFactory factory, ClassLoader targetClassLoader) {
    if (!sameClassLoader(targetClassLoader, payload)) {
      payload = factory.copyUserMessagePayload(targetClassLoader, payload);
    }
    return payload;
  }

  @Override
  public OptionalLong isBarrierMessage() {
    return OptionalLong.empty();
  }

  @Override
  public Message copy(MessageFactory factory) {
    return new SdkMessage(source, target, payload, priority.priority, priority.laxity, type, id);
  }

  @Override
  public void writeTo(MessageFactory factory, DataOutputView target) throws IOException {
    Envelope envelope = envelope(factory);
    factory.serializeEnvelope(envelope, target);
  }

  @Override
  public PriorityObject getPriority() throws Exception {
    return priority;
  }

  @Override
  public void setPriority(Long priority) throws Exception {
    this.priority = new PriorityObject(priority);
  }

  @Override
  public void setRequiresACK(Boolean flag) {
    this.requiresACK = flag;
  }

  @Override
  public void setPriority(Long priority, Long laxity) throws Exception {
    this.priority =
            new PriorityObject(priority, laxity);
  }

  @Override
  public MessageType getMessageType() {
    return type;
  }

  @Override
  public void setMessageType(MessageType type) {
    this.type = type;
  }

  @Override
  public boolean requiresACK() {
    return requiresACK;
  }

  @Override
  public Long getMessageId() {
    return id;
  }

  @Override
  public void setTarget(Address address) {
    this.target = address;
  }

  @Override
  public void setLessor(Address address) {
    this.lessor = address;
  }

  @Override
  public Address getLessor() { return this.lessor; }

  private Envelope envelope(MessageFactory factory) {
    if (cachedEnvelope == null) {
      Builder builder = Envelope.newBuilder();
      if (source != null) {
        builder.setSource(sdkAddressToProtobufAddress(source));
      }
      builder.setTarget(sdkAddressToProtobufAddress(target));
      builder.setPayload(factory.serializeUserMessagePayload(payload));
      builder.setPriority(priority.priority);
      builder.setLaxity(priority.laxity);
      builder.setType(type.ordinal());
      builder.setId(id);
      builder.setRequiresACK(requiresACK);
      if(lessor!=null){
        builder.setLessor(sdkAddressToProtobufAddress(lessor));
      }
      cachedEnvelope = builder.build();
    }
    return cachedEnvelope;
  }

  private static boolean sameClassLoader(ClassLoader targetClassLoader, Object payload) {
    return payload.getClass().getClassLoader() == targetClassLoader;
  }

  private static EnvelopeAddress sdkAddressToProtobufAddress(Address source) {
    return EnvelopeAddress.newBuilder()
        .setNamespace(source.type().namespace())
        .setType(source.type().name())
        .setInternalNamespace(source.type().getInternalType()==null?"":source.type().getInternalType().namespace())
        .setInternalType(source.type().getInternalType()==null?"":source.type().getInternalType().name())
        .setId(source.id())
        .build();
  }

  @Override
  public String toString(){
    return String.format("SdkMessage [source: " + (source==null? "null":source) + " -> " +
            " target: " + (target == null? "null":target) + " priority " + priority
            +" id " + id + " type "+ type + " activation: " + getHostActivation()
            + " requiresACK: " + requiresACK()
            + " lessor " + (lessor==null?"null":lessor) + "]");
  }
}
