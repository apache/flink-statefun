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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.annotation.Internal;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionProvider;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;

@NotThreadSafe
@Internal
final class SerializableHttpFunctionProvider implements SerializableStatefulFunctionProvider {

  private static final long serialVersionUID = 1;

  private final Map<FunctionType, HttpFunctionEndpointSpec> supportedTypes;
  private transient @Nullable HttpFunctionProvider delegate;

  SerializableHttpFunctionProvider(Map<FunctionType, HttpFunctionEndpointSpec> supportedTypes) {
    this.supportedTypes = Objects.requireNonNull(supportedTypes);
  }

  @Override
  public StatefulFunction functionOfType(FunctionType type) {
    if (delegate == null) {
      delegate = new HttpFunctionProvider(supportedTypes, Collections.emptyMap());
    }
    return delegate.functionOfType(type);
  }
}
