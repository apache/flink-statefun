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

package org.apache.flink.statefun.flink.core.httpfn;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.statefun.sdk.state.Expiration;

public final class StateSpec implements Serializable {

  private static final long serialVersionUID = 1;

  private final String name;
  private final Expiration ttlExpiration;

  public StateSpec(String name) {
    this(name, Expiration.none());
  }

  public StateSpec(String name, Expiration ttlExpiration) {
    this.name = Objects.requireNonNull(name);
    this.ttlExpiration = Objects.requireNonNull(ttlExpiration);
  }

  public String name() {
    return name;
  }

  public Expiration ttlExpiration() {
    return ttlExpiration;
  }
}
