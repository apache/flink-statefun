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

package org.apache.flink.statefun.e2e.routablekafka;

import org.apache.flink.statefun.e2e.routablekafka.generated.RoutableKafkaVerification.MessageWithAddress;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;

final class Constants {

  private Constants() {}

  static final String KAFKA_BOOTSTRAP_SERVERS_CONF = "kafka-bootstrap-servers";

  static final EgressIdentifier<MessageWithAddress> EGRESS_ID =
      new EgressIdentifier<>(
          "org.apache.flink.e2e.routablekafka", "tagged-messages", MessageWithAddress.class);

  static final String FUNCTION_NAMESPACE = "org.apache.flink.e2e.routablekafka";
  static final FunctionType FUNCTION_TYPE_ONE = new FunctionType(FUNCTION_NAMESPACE, "t0");
  static final FunctionType FUNCTION_TYPE_TWO = new FunctionType(FUNCTION_NAMESPACE, "t1");
}
