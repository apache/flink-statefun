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

package org.apache.flink.statefun.e2e.sanity;

import org.apache.flink.statefun.e2e.sanity.generated.VerificationMessages.Command;
import org.apache.flink.statefun.e2e.sanity.generated.VerificationMessages.StateSnapshot;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

final class Constants {

  private Constants() {}

  static final String KAFKA_BOOTSTRAP_SERVERS_CONF = "kafka-bootstrap-servers";

  static final IngressIdentifier<Command> COMMAND_INGRESS_ID =
      new IngressIdentifier<>(Command.class, "org.apache.flink.e2e.sanity", "commands");
  static final EgressIdentifier<StateSnapshot> STATE_SNAPSHOT_EGRESS_ID =
      new EgressIdentifier<>("org.apache.flink.e2e.sanity", "state-snapshots", StateSnapshot.class);

  static final FunctionType[] FUNCTION_TYPES =
      new FunctionType[] {
        new FunctionType("org.apache.flink.e2e.sanity", "t0"),
        new FunctionType("org.apache.flink.e2e.sanity", "t1")
      };
}
