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

  public static final String NAMESPACE = "statefun.smoke.e2e";
  public static final String INGRESS_NAME = "command-generator-source";
  public static final String EGRESS_NAME = "discard-sink";
  public static final String VERIFICATION_EGRESS_NAME = "verification-sink";
  public static final String FUNCTION_NAME = "command-interpreter-fn";
  public static final String PROTOBUF_NAMESPACE = "type.googleapis.com";

  public static final IngressIdentifier<TypedValue> IN =
      new IngressIdentifier<>(TypedValue.class, NAMESPACE, INGRESS_NAME);

  public static final EgressIdentifier<TypedValue> OUT =
      new EgressIdentifier<>(NAMESPACE, EGRESS_NAME, TypedValue.class);

  public static final EgressIdentifier<TypedValue> VERIFICATION_RESULT =
      new EgressIdentifier<>(NAMESPACE, VERIFICATION_EGRESS_NAME, TypedValue.class);

  // For embedded/remote functions to bind with the smoke-e2e-common testing framework
  public static final FunctionType FN_TYPE = new FunctionType(NAMESPACE, FUNCTION_NAME);
}
