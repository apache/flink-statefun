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
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.statefun.flink.core.generated.Envelope;
import org.apache.flink.statefun.flink.core.generated.Envelope.Builder;
import org.apache.flink.statefun.flink.core.generated.EnvelopeAddress;
import org.apache.flink.statefun.sdk.Address;

@Internal
public final class SdkMessage implements Message {

  private final Address target;

  @Nullable private final Address source;
  @Nullable private final String cancellationToken;
  @Nullable private Envelope cachedEnvelope;

  private Object payload;

  public SdkMessage(@Nullable Address source, Address target, Object payload) {
    this(source, target, payload, null);
  }

  SdkMessage(
      @Nullable Address source,
      Address target,
      Object payload,
      @Nullable String cancellationToken) {
    this.source = source;
    this.target = Objects.requireNonNull(target);
    this.payload = Objects.requireNonNull(payload);
    this.cancellationToken = cancellationToken;
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
  public Optional<String> cancellationToken() {
    return Optional.ofNullable(cancellationToken);
  }

  @Override
  public Message copy(MessageFactory factory) {
    return new SdkMessage(source, target, payload, cancellationToken);
  }

  @Override
  public void writeTo(MessageFactory factory, DataOutputView target) throws IOException {
    Envelope envelope = envelope(factory);
    factory.serializeEnvelope(envelope, target);
  }

  private Envelope envelope(MessageFactory factory) {
    if (cachedEnvelope == null) {
      Builder builder = Envelope.newBuilder();
      if (source != null) {
        builder.setSource(sdkAddressToProtobufAddress(source));
      }
      builder.setTarget(sdkAddressToProtobufAddress(target));
      builder.setPayload(factory.serializeUserMessagePayload(payload));
      if (cancellationToken != null) {
        builder.setCancellationToken(cancellationToken);
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
        .setId(source.id())
        .build();
  }
}
