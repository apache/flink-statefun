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

import com.google.protobuf.Any;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

public class Constants {

  public static final IngressIdentifier<Any> IN = new IngressIdentifier<>(Any.class, "", "source");

  public static final EgressIdentifier<Any> OUT = new EgressIdentifier<>("", "sink", Any.class);

  public static final FunctionType FN_TYPE = new FunctionType("v", "f1");

  public static final EgressIdentifier<Any> VERIFICATION_RESULT =
      new EgressIdentifier<>("", "verification", Any.class);
}
