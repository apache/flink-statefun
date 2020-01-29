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

public class WebsocketDriverEvent {
  private String driverId;
  private int currentLocation;
  private DriverStatus driverStatus;
  private RideInformation ride;

  public WebsocketDriverEvent() {}

  public WebsocketDriverEvent(
      String driverId, int currentLocation, DriverStatus driverStatus, RideInformation ride) {
    this.driverId = driverId;
    this.currentLocation = currentLocation;
    this.driverStatus = driverStatus;
    this.ride = ride;
  }

  public String getDriverId() {
    return driverId;
  }

  public void setDriverId(String driverId) {
    this.driverId = driverId;
  }

  public int getCurrentLocation() {
    return currentLocation;
  }

  public void setCurrentLocation(int currentLocation) {
    this.currentLocation = currentLocation;
  }

  public DriverStatus getDriverStatus() {
    return driverStatus;
  }

  public void setDriverStatus(DriverStatus driverStatus) {
    this.driverStatus = driverStatus;
  }

  public RideInformation getRide() {
    return ride;
  }

  public void setRide(RideInformation ride) {
    this.ride = ride;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WebsocketDriverEvent that = (WebsocketDriverEvent) o;
    return currentLocation == that.currentLocation
        && Objects.equals(driverId, that.driverId)
        && driverStatus == that.driverStatus
        && Objects.equals(ride, that.ride);
  }

  @Override
  public int hashCode() {
    return Objects.hash(driverId, currentLocation, driverStatus, ride);
  }

  @Override
  public String toString() {
    return "WebsocketDriverEvent{"
        + "driverId='"
        + driverId
        + '\''
        + ", currentLocation="
        + currentLocation
        + ", driverStatus="
        + driverStatus
        + ", ride="
        + ride
        + '}';
  }

  public enum DriverStatus {
    IDLE,
    PICKUP,
    ENROUTE,
    RIDE_COMPLETED
  }

  public static class RideInformation {
    private String passengerId;
    private int pickupLocation;
    private int dropoffLocation;

    public RideInformation() {}

    public RideInformation(String passengerId, int pickupLocation, int dropoffLocation) {
      this.passengerId = passengerId;
      this.pickupLocation = pickupLocation;
      this.dropoffLocation = dropoffLocation;
    }

    public String getPassengerId() {
      return passengerId;
    }

    public void setPassengerId(String passengerId) {
      this.passengerId = passengerId;
    }

    public int getPickupLocation() {
      return pickupLocation;
    }

    public void setPickupLocation(int pickupLocation) {
      this.pickupLocation = pickupLocation;
    }

    public int getDropoffLocation() {
      return dropoffLocation;
    }

    public void setDropoffLocation(int dropoffLocation) {
      this.dropoffLocation = dropoffLocation;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RideInformation that = (RideInformation) o;
      return pickupLocation == that.pickupLocation
          && dropoffLocation == that.dropoffLocation
          && Objects.equals(passengerId, that.passengerId);
    }

    @Override
    public String toString() {
      return "RideInformation{"
          + "passengerId='"
          + passengerId
          + '\''
          + ", pickupLocation="
          + pickupLocation
          + ", dropoffLocation="
          + dropoffLocation
          + '}';
    }

    @Override
    public int hashCode() {
      return Objects.hash(passengerId, pickupLocation, dropoffLocation);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String driverId;
    private int currentLocation;
    private DriverStatus driverStatus;
    private RideInformation ride;

    private Builder() {}

    public Builder driverId(String driverId) {
      this.driverId = driverId;
      return this;
    }

    public Builder currentLocation(int currentLocation) {
      this.currentLocation = currentLocation;
      return this;
    }

    public Builder driverStatus(DriverStatus driverStatus) {
      this.driverStatus = driverStatus;
      return this;
    }

    public Builder ride(RideInformation ride) {
      this.ride = ride;
      return this;
    }

    public WebsocketDriverEvent build() {
      WebsocketDriverEvent websocketDriverEvent = new WebsocketDriverEvent();
      websocketDriverEvent.setDriverId(driverId);
      websocketDriverEvent.setCurrentLocation(currentLocation);
      websocketDriverEvent.setDriverStatus(driverStatus);
      websocketDriverEvent.setRide(ride);
      return websocketDriverEvent;
    }
  }
}
