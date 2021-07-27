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

package org.apache.flink.statefun.flink.datastream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

/** Identifies a generic egress. */
public class GenericEgress {
  private final EgressIdentifier<TypedValue> id;

  private GenericEgress(TypeName typeName) {
    this.id = new EgressIdentifier<>(typeName.name(), typeName.name(), TypedValue.class);
  }

  @Override
  public String toString() {
    return "GenericEgress{" + "typeName=" + id.namespace() + "/" + id.name() + '}';
  }

  @Internal
  EgressIdentifier<TypedValue> getId() {
    return this.id;
  }

  /**
   * Creates a generic egress with the given name.
   *
   * <pre>{@code
   * final GenericEgress egress =
   *    GenericEgress.named(TypeName.parseFrom("example/egress"));
   * }</pre>
   *
   * @param typeName unique name for the GenericEgress.
   * @return a {@link GenericEgress} spec.
   */
  public static GenericEgress named(TypeName typeName) {
    return new GenericEgress(typeName);
  }
}
