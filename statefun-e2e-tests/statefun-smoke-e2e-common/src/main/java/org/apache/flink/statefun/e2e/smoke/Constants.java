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
package org.apache.flink.statefun.e2e.smoke;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public class Constants {

  public static final IngressIdentifier<TypedValue> IN =
      new IngressIdentifier<>(TypedValue.class, "statefun.smoke.e2e", "command-generator-source");

  public static final EgressIdentifier<TypedValue> OUT =
      new EgressIdentifier<>("statefun.smoke.e2e", "discard-sink", TypedValue.class);

  public static final EgressIdentifier<TypedValue> VERIFICATION_RESULT =
      new EgressIdentifier<>("statefun.smoke.e2e", "verification-sink", TypedValue.class);

  // For embedded/remote functions to bind with the smoke-e2e-common testing framework
  public static final FunctionType FN_TYPE = new FunctionType("statefun.smoke.e2e", "f1");
}
