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

import static java.util.Objects.requireNonNull;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.ververica.statefun.flink.core.generated.Payload;
import javax.annotation.Nonnull;

public final class MessagePayloadSerializerMultiLanguage implements MessagePayloadSerializer {

  @Override
  public Object deserialize(@Nonnull ClassLoader targetClassLoader, @Nonnull Payload payload) {
    return Any.newBuilder()
        .setTypeUrl(payload.getClassName())
        .setValue(payload.getPayloadBytes())
        .build();
  }

  @Override
  public Payload serialize(@Nonnull Object what) {
    final Any any = requireAny(what);
    final String className = any.getTypeUrl();
    final ByteString payloadBytes = any.getValue();
    return Payload.newBuilder().setClassName(className).setPayloadBytes(payloadBytes).build();
  }

  @Override
  public Object copy(@Nonnull ClassLoader targetClassLoader, @Nonnull Object what) {
    requireNonNull(targetClassLoader);
    Any any = requireAny(what);
    return any.toBuilder().build();
  }

  private static Any requireAny(Object what) {
    if (what instanceof Any) {
      return (Any) what;
    }
    if (what == null) {
      throw new IllegalArgumentException("Unable to handle a NULL payload value.");
    }
    throw new IllegalStateException(
        "The payload "
            + what.getClass().getCanonicalName()
            + " is not of type "
            + Any.class.getCanonicalName());
  }
}
