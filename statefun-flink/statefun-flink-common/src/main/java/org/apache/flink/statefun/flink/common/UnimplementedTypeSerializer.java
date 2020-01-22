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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

final class UnimplementedTypeSerializer<T> extends TypeSerializer<T> {

  private static final long serialVersionUID = 1L;

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<T> duplicate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public T createInstance() {
    throw new UnsupportedOperationException();
  }

  @Override
  public T copy(T t) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T copy(T t, T t1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getLength() {
    return 0;
  }

  @Override
  public void serialize(T t, DataOutputView dataOutputView) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T deserialize(DataInputView dataInputView) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T deserialize(T t, DataInputView dataInputView) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copy(DataInputView dataInputView, DataOutputView dataOutputView) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object o) {
    return o == this;
  }

  @Override
  public int hashCode() {
    return 7;
  }

  @Override
  public TypeSerializerSnapshot<T> snapshotConfiguration() {
    throw new UnsupportedOperationException();
  }
}
