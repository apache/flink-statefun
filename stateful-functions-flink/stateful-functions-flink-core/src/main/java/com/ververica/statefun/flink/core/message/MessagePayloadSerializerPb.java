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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.ververica.statefun.flink.core.generated.Payload;
import com.ververica.statefun.flink.core.types.protobuf.ProtobufReflectionUtil;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashMap;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class MessagePayloadSerializerPb implements MessagePayloadSerializer {

  private final ObjectOpenHashMap<String, ObjectOpenHashMap<ClassLoader, Parser<? extends Message>>>
      PARSER_CACHE = new ObjectOpenHashMap<>();

  @Override
  public Object deserialize(@Nonnull ClassLoader targetClassLoader, @Nonnull Payload payload) {
    try {
      Parser<? extends Message> parser =
          parserForClassName(targetClassLoader, payload.getClassName());
      return parser.parseFrom(payload.getPayloadBytes());
    } catch (InvalidProtocolBufferException | ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Payload serialize(@Nonnull Object what) {
    final Message message = (Message) what;
    final String className = what.getClass().getName();
    final ByteString body = message.toByteString();

    return Payload.newBuilder().setClassName(className).setPayloadBytes(body).build();
  }

  @Override
  public Object copy(@Nonnull ClassLoader targetClassLoader, @Nonnull Object what) {
    Objects.requireNonNull(targetClassLoader);
    if (!(what instanceof Message)) {
      throw new IllegalStateException();
    }
    Message message = (Message) what;
    ByteString messageBytes = message.toByteString();
    try {
      Parser<? extends Message> parser =
          parserForClassName(targetClassLoader, what.getClass().getName());
      return parser.parseFrom(messageBytes);
    } catch (InvalidProtocolBufferException | ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  private Parser<? extends Message> parserForClassName(
      ClassLoader userCodeClassLoader, String messageClassName) throws ClassNotFoundException {

    ObjectOpenHashMap<ClassLoader, Parser<? extends Message>> classLoaders =
        PARSER_CACHE.get(messageClassName);
    if (classLoaders == null) {
      PARSER_CACHE.put(messageClassName, classLoaders = new ObjectOpenHashMap<>());
    }
    Parser<? extends Message> parser = classLoaders.get(userCodeClassLoader);
    if (parser == null) {
      classLoaders.put(
          userCodeClassLoader, parser = findParser(userCodeClassLoader, messageClassName));
    }
    return parser;
  }

  private Parser<? extends Message> findParser(
      ClassLoader userCodeClassLoader, String messageClassName) throws ClassNotFoundException {
    Class<? extends Message> messageType =
        Class.forName(messageClassName, true, userCodeClassLoader).asSubclass(Message.class);

    return ProtobufReflectionUtil.protobufParser(messageType);
  }
}
