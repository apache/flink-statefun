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

import static java.util.stream.Collectors.toMap;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import java.util.Map;
import java.util.function.Function;

/**
 * Extract and resolve all the {@link Descriptors.FileDescriptor} embedded in a {@link
 * DescriptorProtos.FileDescriptorSet}.
 */
final class FileDescriptorResolver {

  static Map<String, Descriptors.FileDescriptor> resolve(
      DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
    FileDescriptorResolver resolver = new FileDescriptorResolver(fileDescriptorSet);
    return resolver.resolve();
  }

  private final Map<String, FileDescriptorProtoWrapper> descriptorByName;

  private FileDescriptorResolver(DescriptorProtos.FileDescriptorSet descriptorSet) {
    // dependencies are specified in the form of a proto filename, therefore we need to index the
    // FileDescriptorProto's in that file by their name.
    this.descriptorByName =
        descriptorSet.getFileList().stream()
            .map(FileDescriptorProtoWrapper::new)
            .collect(toMap(FileDescriptorProtoWrapper::name, Function.identity()));
  }

  /** Resolve each {@code FileDescriptorProto} by name. */
  private Map<String, Descriptors.FileDescriptor> resolve() {
    return descriptorByName.entrySet().stream()
        .collect(toMap(Map.Entry::getKey, e -> e.getValue().resolve()));
  }

  private final class FileDescriptorProtoWrapper {
    private final DescriptorProtos.FileDescriptorProto unresolved;
    private Descriptors.FileDescriptor resolved;

    private FileDescriptorProtoWrapper(FileDescriptorProto unresolved) {
      this.unresolved = unresolved;
    }

    public String name() {
      return unresolved.getName();
    }

    /** Resolve a given {@code FileDescriptorProto} */
    private Descriptors.FileDescriptor resolve() {
      if (resolved != null) {
        return resolved;
      }
      Descriptors.FileDescriptor[] dependencies =
          unresolved.getDependencyList().stream()
              .map(descriptorByName::get)
              .map(FileDescriptorProtoWrapper::resolve)
              .toArray(Descriptors.FileDescriptor[]::new);
      try {
        Descriptors.FileDescriptor resolved =
            Descriptors.FileDescriptor.buildFrom(unresolved, dependencies, false);
        this.resolved = resolved;
        return resolved;
      } catch (Descriptors.DescriptorValidationException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
