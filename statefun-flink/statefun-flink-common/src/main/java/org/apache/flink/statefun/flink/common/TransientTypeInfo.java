/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.flink.common;

import java.util.Objects;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * A wrapper that prevents the underlying type information from being serialized. This is important
 * when the type info is needed to construct the StreamGraph but the classes may not exist on the
 * TaskManager user-code classloader.
 */
public class TransientTypeInfo<T> extends TypeInformation<T> {

  private static final long serialVersionUID = 1L;

  private final transient TypeInformation<T> inner;

  public TransientTypeInfo(TypeInformation<T> inner) {
    this.inner = Objects.requireNonNull(inner);
  }

  private TypeInformation<T> getInner() {
    if (inner == null) {
      throw new RuntimeException(
          "This method should only ever be called when generating"
              + " the Flink StreamGraph. This is bug, please file a ticket with the project maintainers");
    }

    return inner;
  }

  @Override
  public boolean isBasicType() {
    return getInner().isBasicType();
  }

  @Override
  public boolean isTupleType() {
    return getInner().isTupleType();
  }

  @Override
  public int getArity() {
    return getInner().getArity();
  }

  @Override
  public int getTotalFields() {
    return getInner().getTotalFields();
  }

  @Override
  public Class<T> getTypeClass() {
    return getInner().getTypeClass();
  }

  @Override
  public boolean isKeyType() {
    return getInner().isKeyType();
  }

  @Override
  public TypeSerializer<T> createSerializer(ExecutionConfig config) {
    return getInner().createSerializer(config);
  }

  @Override
  public String toString() {
    return "TransientTypeInfo(" + getInner().toString() + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }

    if (getClass() != o.getClass()) {
      return getInner().equals(o);
    } else {
      TransientTypeInfo<?> that = (TransientTypeInfo<?>) o;
      return Objects.equals(inner, that.inner);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getInner());
  }

  @Override
  public boolean canEqual(Object obj) {
    return equals(obj);
  }
}
