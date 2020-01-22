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
package org.apache.flink.statefun.flink.common;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public final class UnimplementedTypeInfo<T> extends TypeInformation<T> {

  private static final long serialVersionUID = 1;

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return 0;
  }

  @Override
  public int getTotalFields() {
    return 0;
  }

  @Override
  public Class<T> getTypeClass() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isKeyType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
    return new UnimplementedTypeSerializer<>();
  }

  @Override
  public String toString() {
    return "UnimplementedTypeInfo";
  }

  @Override
  public boolean equals(Object o) {
    return o == this;
  }

  @Override
  public int hashCode() {
    return 1337;
  }

  @Override
  public boolean canEqual(Object o) {
    return o instanceof UnimplementedTypeInfo;
  }
}
