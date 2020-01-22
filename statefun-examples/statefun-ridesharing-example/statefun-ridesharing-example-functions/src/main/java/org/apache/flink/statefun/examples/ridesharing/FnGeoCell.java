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

import org.apache.flink.statefun.examples.ridesharing.generated.*;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedValue;

public class FnGeoCell implements StatefulFunction {
  static final FunctionType TYPE = new FunctionType(Identifiers.NAMESPACE, "geo-cell");

  @Persisted
  private final PersistedValue<GeoCellState> drivers =
      PersistedValue.of("drivers", GeoCellState.class);

  @Override
  public void invoke(Context context, Object input) {
    Address caller = context.caller();
    if (input instanceof JoinCell) {
      addDriver(caller);
    } else if (input instanceof LeaveCell) {
      removeDriver(caller);
    } else if (input instanceof GetDriver) {
      getDriver(context);
    } else {
      throw new IllegalStateException("Unknown message type " + input);
    }
  }

  private void getDriver(Context context) {
    final GeoCellState state = drivers.get();

    if (hasDriver(state)) {
      String nextDriverId = state.getDriverIdList().get(0);
      context.reply(DriverInCell.newBuilder().setDriverId(nextDriverId).build());
    } else {
      context.reply(DriverInCell.newBuilder().build());
    }
  }

  private void addDriver(Address driver) {
    GeoCellState state = drivers.get();
    if (state == null) {
      state = GeoCellState.newBuilder().addDriverId(driver.id()).build();
    } else {
      state = state.toBuilder().addDriverId(driver.id()).build();
    }
    drivers.set(state);
  }

  private void removeDriver(Address driver) {
    GeoCellState state = drivers.get();
    if (state == null) {
      return;
    }
    GeoCellState.Builder nextState = state.toBuilder();
    nextState.clearDriverId();

    for (String otherDriverID : state.getDriverIdList()) {
      if (!otherDriverID.equals(driver.id())) {
        nextState.addDriverId(otherDriverID);
      }
    }
    drivers.set(nextState.build());
  }

  private boolean hasDriver(GeoCellState registeredDrivers) {
    return registeredDrivers != null && !registeredDrivers.getDriverIdList().isEmpty();
  }
}
