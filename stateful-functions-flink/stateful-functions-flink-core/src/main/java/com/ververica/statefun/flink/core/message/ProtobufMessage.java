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

package com.ververica.statefun.flink.core.message;

import com.ververica.statefun.flink.core.generated.Envelope;
import com.ververica.statefun.flink.core.generated.EnvelopeAddress;
import com.ververica.statefun.sdk.Address;
import com.ververica.statefun.sdk.FunctionType;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.core.memory.DataOutputView;

final class ProtobufMessage implements Message {

  private final Envelope envelope;

  private Address source;
  private Address target;
  private Object payload;

  ProtobufMessage(Envelope envelope) {
    this.envelope = Objects.requireNonNull(envelope);
  }

  @Override
  @Nullable
  public Address source() {
    if (source != null) {
      return source;
    }
    if ((source = protobufAddressToSdkAddress(envelope.getSource())) == null) {
      return null;
    }
    return source;
  }

  @Override
  public Address target() {
    if (target != null) {
      return target;
    }
    if ((target = protobufAddressToSdkAddress(envelope.getTarget())) == null) {
      throw new IllegalStateException("A mandatory target address is missing");
    }
    return target;
  }

  @Override
  public Object payload(MessageFactory factory, ClassLoader targetClassLoader) {
    if (payload == null) {
      payload = factory.deserializeUserMessagePayload(targetClassLoader, envelope.getPayload());
    } else if (!sameClassLoader(targetClassLoader, payload)) {
      payload = factory.copyUserMessagePayload(targetClassLoader, payload);
    }
    return payload;
  }

  @Override
  public boolean isBarrierMessage() {
    return envelope.hasCheckpoint();
  }

  @Override
  public Message copy(MessageFactory unused) {
    return new ProtobufMessage(envelope);
  }

  @Override
  public void writeTo(MessageFactory factory, DataOutputView target) throws IOException {
    Objects.requireNonNull(target);
    factory.serializeEnvelope(envelope, target);
  }

  private static boolean sameClassLoader(ClassLoader targetClassLoader, Object payload) {
    return payload.getClass().getClassLoader() == targetClassLoader;
  }

  @Nullable
  private static Address protobufAddressToSdkAddress(EnvelopeAddress address) {
    if (address == null
        || (address.getId().isEmpty()
            && address.getNamespace().isEmpty()
            && address.getType().isEmpty())) {
      return null;
    }
    FunctionType functionType = new FunctionType(address.getNamespace(), address.getType());
    return new Address(functionType, address.getId());
  }
}
