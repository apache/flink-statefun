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

import com.google.protobuf.DynamicMessage;
import java.util.List;
import java.util.function.Function;

final class ProtobufDynamicMessageLens implements Function<DynamicMessage, Object> {
  private final PathFragmentDescriptor[] path;
  private final PathFragmentDescriptor value;

  ProtobufDynamicMessageLens(List<PathFragmentDescriptor> path) {
    this.path = path.subList(0, path.size() - 1).toArray(new PathFragmentDescriptor[0]);
    this.value = path.get(path.size() - 1);
  }

  @Override
  public Object apply(DynamicMessage message) {
    message = traverseToTheLastMessage(message);
    return value.value(message);
  }

  /**
   * Traverse the path from root to the last nested message. At each traversed depth follow the next
   * FiledDescriptor specified in descriptorPath for that depth. The returned message would be the
   * last Message which contains the desired value. For example the path defined by: {@code .a.b.c}
   * would result with {@code b} returned.
   */
  private DynamicMessage traverseToTheLastMessage(DynamicMessage root) {
    for (PathFragmentDescriptor p : path) {
      root = (DynamicMessage) p.value(root);
    }
    return root;
  }
}
