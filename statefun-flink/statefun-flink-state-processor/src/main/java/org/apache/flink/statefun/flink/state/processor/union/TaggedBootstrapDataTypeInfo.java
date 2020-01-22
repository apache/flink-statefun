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
package org.apache.flink.statefun.flink.state.processor.union;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Preconditions;

/** Type information for {@link TaggedBootstrapData}. */
public final class TaggedBootstrapDataTypeInfo extends TypeInformation<TaggedBootstrapData> {

  private static final long serialVersionUID = 1L;

  private final List<TypeInformation<?>> payloadTypeInfos;

  TaggedBootstrapDataTypeInfo(List<TypeInformation<?>> payloadTypeInfos) {
    Preconditions.checkNotNull(payloadTypeInfos);
    Preconditions.checkArgument(!payloadTypeInfos.isEmpty());
    this.payloadTypeInfos = payloadTypeInfos;
  }

  @Override
  public TypeSerializer<TaggedBootstrapData> createSerializer(ExecutionConfig executionConfig) {
    final List<TypeSerializer<?>> payloadSerializers =
        payloadTypeInfos.stream()
            .map(typeInfo -> typeInfo.createSerializer(executionConfig))
            .collect(Collectors.toList());

    return new TaggedBootstrapDataSerializer(payloadSerializers);
  }

  @Override
  public int getTotalFields() {
    return 1;
  }

  @Override
  public int getArity() {
    return 1;
  }

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public Class<TaggedBootstrapData> getTypeClass() {
    return TaggedBootstrapData.class;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("TaggedBootstrapDataTypeInfo {");
    final int size = payloadTypeInfos.size();
    for (int i = 0; i < size; i++) {
      sb.append(payloadTypeInfos.get(i).toString());
      if (i < size - 1) {
        sb.append(", ");
      }
    }
    sb.append(" }");
    return sb.toString();
  }

  @Override
  public boolean canEqual(Object o) {
    return o instanceof TaggedBootstrapDataTypeInfo;
  }

  @Override
  public int hashCode() {
    return Objects.hash(payloadTypeInfos);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TaggedBootstrapDataTypeInfo that = (TaggedBootstrapDataTypeInfo) o;
    return Objects.equals(payloadTypeInfos, that.payloadTypeInfos);
  }
}
