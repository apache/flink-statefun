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

package com.ververica.statefun.flink.core.types.protobuf;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.ververica.statefun.flink.core.generated.ProtobufSerializerSnapshot;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

@NotThreadSafe
public final class ProtobufSerializer<M extends Message> {

  private final OutputStreamView output;
  private final InputStreamView input;
  private final Parser<M> parser;
  private final ProtobufSerializerSnapshot snapshot;

  public static <M extends Message> ProtobufSerializer<M> forMessageGeneratedClass(Class<M> type) {
    Objects.requireNonNull(type);
    Parser<M> parser = ProtobufReflectionUtil.protobufParser(type);
    ProtobufSerializerSnapshot snapshot = createSnapshot(type);
    return new ProtobufSerializer<>(parser, snapshot);
  }

  private ProtobufSerializer(Parser<M> parser, ProtobufSerializerSnapshot snapshot) {
    this.parser = Objects.requireNonNull(parser);
    this.snapshot = Objects.requireNonNull(snapshot);
    this.input = new InputStreamView();
    this.output = new OutputStreamView();
  }

  public void serialize(M record, DataOutputView target) throws IOException {
    final int size = record.getSerializedSize();
    target.writeInt(size);

    output.set(target);
    try {
      record.writeTo(output);
    } finally {
      output.done();
    }
  }

  public M deserialize(DataInputView source) throws IOException {
    final int serializedSize = source.readInt();
    input.set(source, serializedSize);
    try {
      return parser.parseFrom(input);
    } finally {
      input.done();
    }
  }

  public void copy(DataInputView source, DataOutputView target) throws IOException {
    int serializedSize = source.readInt();
    target.writeInt(serializedSize);
    target.write(source, serializedSize);
  }

  ProtobufSerializerSnapshot snapshot() {
    return snapshot;
  }

  public ProtobufSerializer<M> duplicate() {
    return new ProtobufSerializer<>(parser, snapshot);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------------------------------------------

  private static <M extends Message> ProtobufSerializerSnapshot createSnapshot(Class<M> type) {
    Descriptor messageDescriptor = ProtobufReflectionUtil.protobufDescriptor(type);
    FileDescriptorSet dependencies =
        ProtobufReflectionUtil.protoFileDescriptorSet(messageDescriptor);

    return ProtobufSerializerSnapshot.newBuilder()
        .setMessageName(messageDescriptor.getFullName())
        .setGeneratedJavaName(type.getName())
        .setDescriptorSet(dependencies)
        .build();
  }
}
