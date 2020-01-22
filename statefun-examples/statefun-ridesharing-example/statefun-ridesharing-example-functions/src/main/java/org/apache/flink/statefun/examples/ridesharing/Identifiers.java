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
package org.apache.flink.statefun.examples.ridesharing;

import org.apache.flink.statefun.examples.ridesharing.generated.InboundDriverMessage;
import org.apache.flink.statefun.examples.ridesharing.generated.InboundPassengerMessage;
import org.apache.flink.statefun.examples.ridesharing.generated.OutboundDriverMessage;
import org.apache.flink.statefun.examples.ridesharing.generated.OutboundPassengerMessage;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

final class Identifiers {

  static final String NAMESPACE = "org.apache.flink.statefun.examples.ridesharing";

  static final IngressIdentifier<InboundPassengerMessage> FROM_PASSENGERS =
      new IngressIdentifier<>(InboundPassengerMessage.class, NAMESPACE, "from-passenger");

  static final IngressIdentifier<InboundDriverMessage> FROM_DRIVER =
      new IngressIdentifier<>(InboundDriverMessage.class, NAMESPACE, "from-driver");

  static EgressIdentifier<OutboundPassengerMessage> TO_PASSENGER_EGRESS =
      new EgressIdentifier<>(NAMESPACE, "to-passenger", OutboundPassengerMessage.class);

  static EgressIdentifier<OutboundDriverMessage> TO_OUTBOUND_DRIVER =
      new EgressIdentifier<>(NAMESPACE, "to-driver", OutboundDriverMessage.class);
}
