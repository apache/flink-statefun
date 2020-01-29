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
package org.apache.flink.statefun.examples.ridesharing.simulator.simulation;

import com.google.common.util.concurrent.RateLimiter;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.statefun.examples.ridesharing.simulator.simulation.engine.Scheduler;
import org.apache.flink.statefun.examples.ridesharing.simulator.simulation.engine.Simulatee;
import org.apache.flink.statefun.examples.ridesharing.simulator.simulation.messaging.Communication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class Simulation {
  private final Scheduler scheduler;
  private final Communication communication;
  private final int gridDimension;
  private final int driverCount;
  private final int passengerCount;

  @Autowired
  public Simulation(
      Scheduler scheduler,
      Communication communication,
      @Value("${simulation.grid}") int gridDimension,
      @Value("${simulation.drivers}") int driverCount,
      @Value("${simulation.passengers}") int passengerCount) {
    this.scheduler = Objects.requireNonNull(scheduler);
    this.communication = Objects.requireNonNull(communication);
    this.gridDimension = gridDimension;
    this.driverCount = driverCount;
    this.passengerCount = passengerCount;
  }

  public void start() {
    if (!scheduler.start()) {
      return;
    }
    //
    // create the drivers
    //
    createDrivers();
    createPassengers();
  }

  private void createDrivers() {
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int i = 0; i < driverCount; i++) {
      final String id = "driver-" + UUID.randomUUID();
      final int startLocation = random.nextInt(gridDimension * gridDimension);

      Driver driver = new Driver(id, communication, gridDimension, startLocation);
      scheduler.add(driver);
    }
  }

  private void createPassengers() {
    Thread t = new Thread(new PassengerLoop());
    t.setDaemon(true);
    t.start();
  }

  private final class PassengerLoop implements Runnable {

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void run() {
      final ThreadLocalRandom random = ThreadLocalRandom.current();
      while (true) {
        try {
          createBatchOfPassengers(random);
        } catch (Throwable ignored) {
        }
      }
    }

    private void createBatchOfPassengers(ThreadLocalRandom random) {
      @SuppressWarnings("UnstableApiUsage")
      RateLimiter rate = RateLimiter.create(2);
      for (int j = 0; j < passengerCount; j++) {
        rate.acquire();
        int startCell, endCell;
        do {
          startCell = random.nextInt(gridDimension * gridDimension);
          int dx = random.nextInt(-10, 10);
          int dy = random.nextInt(-10, 10);
          endCell = moveSlightly(startCell, dx, dy);
        } while (startCell == endCell);

        String id = "passenger-" + UUID.randomUUID();
        Simulatee passenger = new Passenger(communication, id, startCell, endCell);
        scheduler.add(passenger);
      }
    }

    private int moveSlightly(int startCell, int dx, int dy) {
      int x = startCell / gridDimension;
      int y = startCell % gridDimension;

      x += dx;
      y += dy;

      if (x < 0) {
        x = 0;
      }
      if (x >= gridDimension) {
        x = gridDimension - 1;
      }
      if (y < 0) {
        y = 0;
      }
      if (y >= gridDimension) {
        y = gridDimension - 1;
      }
      return x * gridDimension + y;
    }
  }
}
