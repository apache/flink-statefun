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
package org.apache.flink.statefun.flink.core;

import org.apache.flink.statefun.flink.core.generated.EnvelopeAddress;
import org.apache.flink.statefun.flink.core.message.MessageFactory;
import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;

@SuppressWarnings("WeakerAccess")
public class TestUtils {

  public static final MessageFactory ENVELOPE_FACTORY =
      MessageFactory.forKey(MessageFactoryKey.forType(MessageFactoryType.WITH_KRYO_PAYLOADS, null));

  public static final FunctionType FUNCTION_TYPE = new FunctionType("test", "a");
  public static final Address FUNCTION_1_ADDR = new Address(FUNCTION_TYPE, "a-1");
  public static final Address FUNCTION_2_ADDR = new Address(FUNCTION_TYPE, "a-2");
  public static final EnvelopeAddress DUMMY_PAYLOAD =
      EnvelopeAddress.newBuilder().setNamespace("com.foo").setType("greet").setId("user-1").build();
}
