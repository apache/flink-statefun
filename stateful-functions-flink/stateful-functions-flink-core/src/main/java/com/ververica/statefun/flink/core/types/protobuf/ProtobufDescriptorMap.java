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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class ProtobufDescriptorMap {

  public static ProtobufDescriptorMap from(String fileDescriptorPath) throws IOException {
    File file = new File(fileDescriptorPath);
    byte[] descriptorBytes = Files.readAllBytes(file.toPath());
    DescriptorProtos.FileDescriptorSet fileDescriptorSet =
        DescriptorProtos.FileDescriptorSet.parseFrom(descriptorBytes);
    return from(fileDescriptorSet);
  }

  public static ProtobufDescriptorMap from(DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
    Map<String, Descriptors.FileDescriptor> resolvedSet =
        FileDescriptorResolver.resolve(fileDescriptorSet);

    Map<String, Descriptors.GenericDescriptor> messageOrEnumDescriptors = new HashMap<>();

    for (Descriptors.FileDescriptor fileDescriptor : resolvedSet.values()) {
      addMessages(messageOrEnumDescriptors, fileDescriptor, packageName(fileDescriptor));
      addEnums(messageOrEnumDescriptors, fileDescriptor, packageName(fileDescriptor));
    }
    return new ProtobufDescriptorMap(messageOrEnumDescriptors);
  }

  private final Map<String, Descriptors.GenericDescriptor> descriptorByName;

  private ProtobufDescriptorMap(Map<String, Descriptors.GenericDescriptor> descriptorByName) {
    this.descriptorByName = descriptorByName;
  }

  public Optional<Descriptors.GenericDescriptor> getDescriptorByName(String messageFullName) {
    Descriptors.GenericDescriptor descriptor = descriptorByName.get(messageFullName);
    return Optional.ofNullable(descriptor);
  }

  private static String packageName(Descriptors.FileDescriptor proto) {
    String packageName = proto.getPackage();
    if (!packageName.isEmpty()) {
      packageName = packageName + ".";
    }
    return packageName;
  }

  private static void addMessages(
      Map<String, Descriptors.GenericDescriptor> descriptors,
      Descriptors.FileDescriptor proto,
      String packageName) {
    for (Descriptors.Descriptor message : proto.getMessageTypes()) {
      String fullName = packageName + message.getName();
      descriptors.put(fullName, message);
    }
  }

  private static void addEnums(
      Map<String, Descriptors.GenericDescriptor> descriptors,
      Descriptors.FileDescriptor descriptor,
      String packageName) {
    for (Descriptors.EnumDescriptor message : descriptor.getEnumTypes()) {
      String fullName = packageName + message.getName();
      descriptors.put(fullName, message);
    }
  }
}
