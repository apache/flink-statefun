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

package com.ververica.statefun.flink.common.protobuf;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet.Builder;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

public final class ProtobufReflectionUtil {
  private ProtobufReflectionUtil() {}

  @SuppressWarnings("unchecked")
  public static <M extends Message> Parser<M> protobufParser(Class<M> messageClass) {
    Object parser = getParserFromGeneratedMessage(messageClass);
    if (!(parser instanceof Parser)) {
      throw new IllegalStateException(
          "was expecting a protobuf parser to be return from the static parser() method on the type  "
              + messageClass
              + " but instead got "
              + parser);
    }
    return (Parser<M>) parser;
  }

  static FileDescriptorSet protoFileDescriptorSet(Descriptor descriptor) {
    Set<FileDescriptor> descriptors = new HashSet<>();
    descriptors.add(descriptor.getFile());
    addDependenciesRecursively(descriptors, descriptor.getFile());

    Builder fileDescriptorSet = FileDescriptorSet.newBuilder();
    for (FileDescriptor d : descriptors) {
      fileDescriptorSet.addFile(d.toProto());
    }
    return fileDescriptorSet.build();
  }

  /** extract the {@linkplain Descriptor} for the generated message type. */
  static <M extends Message> Descriptor protobufDescriptor(Class<M> type) {
    try {
      Method getDescriptor = type.getDeclaredMethod("getDescriptor");
      return (Descriptor) getDescriptor.invoke(type);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(
          "unable to obtain protobuf type fileDescriptorSet for " + type, e);
    }
  }

  /**
   * extracts the {@linkplain Parser} implementation for that type. see:
   * https://developers.google.com/protocol-buffers/docs/reference/java-generated
   */
  private static <M extends Message> Object getParserFromGeneratedMessage(Class<M> messageClass) {
    try {
      Method parserMethod = messageClass.getDeclaredMethod("parser");
      return parserMethod.invoke(parserMethod);
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static void addDependenciesRecursively(
      Set<FileDescriptor> visited, FileDescriptor descriptor) {
    for (FileDescriptor dependency : descriptor.getDependencies()) {
      if (visited.add(dependency)) {
        addDependenciesRecursively(visited, dependency.getFile());
      }
    }
  }
}
