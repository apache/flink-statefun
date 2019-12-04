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
package com.ververica.statefun.flink.core.types.protobuf.protopath;

import com.google.protobuf.Descriptors;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

final class ProtobufPathCompiler {

  static List<PathFragmentDescriptor> compile(
      Descriptors.Descriptor messageDescriptor, List<PathFragment> pathFragments) {
    List<PathFragmentDescriptor> accessors = new ArrayList<>();
    for (int i = 0; i < pathFragments.size(); i++) {
      PathFragment pathFragment = pathFragments.get(i);
      Descriptors.FieldDescriptor f = findFieldByName(messageDescriptor, pathFragment);
      accessors.add(new PathFragmentDescriptor(f, pathFragment));
      if (i < pathFragments.size() - 1) {
        // all fragments expect the last one in the path
        // are of Message type (the last one can be primitive)
        messageDescriptor = f.getMessageType();
      }
    }
    return accessors;
  }

  @Nonnull
  private static Descriptors.FieldDescriptor findFieldByName(
      Descriptors.Descriptor messageDescriptor, PathFragment pathFragment) {
    @Nullable
    Descriptors.FieldDescriptor actualField =
        messageDescriptor.findFieldByName(pathFragment.getName());
    if (actualField == null) {
      throw new IllegalStateException(
          "Unable to find the field "
              + pathFragment.getName()
              + " on "
              + messageDescriptor.getFullName());
    }
    if (pathFragment.isRepeated() && !actualField.isRepeated()) {
      throw new IllegalArgumentException(
          "Can't index into a non repeated field " + actualField.getFullName());
    }
    return actualField;
  }
}
