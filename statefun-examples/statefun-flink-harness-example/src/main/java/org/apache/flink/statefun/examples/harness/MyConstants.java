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
package org.apache.flink.statefun.examples.harness;

import org.apache.flink.statefun.examples.harness.MyMessages.MyInputMessage;
import org.apache.flink.statefun.examples.harness.MyMessages.MyOutputMessage;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

final class MyConstants {
  static final IngressIdentifier<MyInputMessage> REQUEST_INGRESS =
      new IngressIdentifier<>(
          MyInputMessage.class, "org.apache.flink.statefun.examples.harness", "in");

  static final EgressIdentifier<MyOutputMessage> RESULT_EGRESS =
      new EgressIdentifier<>(
          "org.apache.flink.statefun.examples.harness", "out", MyOutputMessage.class);

  static final FunctionType MY_FUNCTION_TYPE =
      new FunctionType("org.apache.flink.statefun.examples.harness", "my-function");
}
