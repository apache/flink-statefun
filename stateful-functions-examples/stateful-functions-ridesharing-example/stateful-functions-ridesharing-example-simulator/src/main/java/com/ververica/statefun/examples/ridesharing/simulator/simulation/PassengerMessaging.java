/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.examples.ridesharing.simulator.simulation;

import com.ververica.statefun.examples.ridesharing.generated.InboundPassengerMessage;
import com.ververica.statefun.examples.ridesharing.generated.OutboundPassengerMessage;
import com.ververica.statefun.examples.ridesharing.simulator.model.WebsocketPassengerEvent;

public interface PassengerMessaging {

  /** handle an event that was sent from application to the simulator */
  void incomingPassengerEvent(OutboundPassengerMessage message);

  /** send an event to the application */
  void outgoingPassengerEvent(InboundPassengerMessage message);

  /** notify whoever is listening that there is a passenger state change */
  void broadcastPassengerSimulationEvent(WebsocketPassengerEvent passengerEvent);
}
