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
package org.apache.flink.statefun.examples.ridesharing.simulator.model;

import java.util.Objects;

public class WebsocketPassengerEvent {
  private String passengerId;
  private PassengerStatus status;
  private String rideId;
  private String driverId;
  private int startLocation;
  private int endLocation;

  public WebsocketPassengerEvent() {}

  public WebsocketPassengerEvent(
      String passengerId,
      PassengerStatus status,
      String rideId,
      String driverId,
      int startLocation,
      int endLocation) {
    this.passengerId = passengerId;
    this.status = status;
    this.rideId = rideId;
    this.driverId = driverId;
    this.startLocation = startLocation;
    this.endLocation = endLocation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WebsocketPassengerEvent that = (WebsocketPassengerEvent) o;
    return startLocation == that.startLocation
        && endLocation == that.endLocation
        && Objects.equals(passengerId, that.passengerId)
        && status == that.status
        && Objects.equals(rideId, that.rideId)
        && Objects.equals(driverId, that.driverId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(passengerId, status, rideId, driverId, startLocation, endLocation);
  }

  public String getPassengerId() {
    return passengerId;
  }

  public void setPassengerId(String passengerId) {
    this.passengerId = passengerId;
  }

  public PassengerStatus getStatus() {
    return status;
  }

  public void setStatus(PassengerStatus status) {
    this.status = status;
  }

  public String getRideId() {
    return rideId;
  }

  public void setRideId(String rideId) {
    this.rideId = rideId;
  }

  public String getDriverId() {
    return driverId;
  }

  public void setDriverId(String driverId) {
    this.driverId = driverId;
  }

  public int getStartLocation() {
    return startLocation;
  }

  public void setStartLocation(int startLocation) {
    this.startLocation = startLocation;
  }

  public int getEndLocation() {
    return endLocation;
  }

  public void setEndLocation(int endLocation) {
    this.endLocation = endLocation;
  }

  public enum PassengerStatus {
    IDLE,
    REQUESTING,
    WAITING_FOR_RIDE_TO_START,
    FAIL,
    RIDING,
    DONE
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String passengerId;
    private PassengerStatus status;
    private String rideId;
    private String driverId;
    private int startLocation;
    private int endLocation;

    private Builder() {}

    public Builder passengerId(String passengerId) {
      this.passengerId = passengerId;
      return this;
    }

    public Builder status(PassengerStatus status) {
      this.status = status;
      return this;
    }

    public Builder rideId(String rideId) {
      this.rideId = rideId;
      return this;
    }

    public Builder driverId(String driverId) {
      this.driverId = driverId;
      return this;
    }

    public Builder startLocation(int startLocation) {
      this.startLocation = startLocation;
      return this;
    }

    public Builder endLocation(int endLocation) {
      this.endLocation = endLocation;
      return this;
    }

    public WebsocketPassengerEvent build() {
      WebsocketPassengerEvent websocketPassengerEvent = new WebsocketPassengerEvent();
      websocketPassengerEvent.setPassengerId(passengerId);
      websocketPassengerEvent.setStatus(status);
      websocketPassengerEvent.setRideId(rideId);
      websocketPassengerEvent.setDriverId(driverId);
      websocketPassengerEvent.setStartLocation(startLocation);
      websocketPassengerEvent.setEndLocation(endLocation);
      return websocketPassengerEvent;
    }
  }
}
