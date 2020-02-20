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

package org.apache.flink.statefun.flink.core.backpressure;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.flink.statefun.flink.core.TestUtils;
import org.junit.Test;

public class ThresholdBackPressureValveTest {

  @Test
  public void simpleUsage() {
    ThresholdBackPressureValve valve = new ThresholdBackPressureValve(2);

    valve.notifyAsyncOperationRegistered();
    valve.notifyAsyncOperationRegistered();

    assertTrue(valve.shouldBackPressure());
  }

  @Test
  public void completedOperationReleaseBackpressure() {
    ThresholdBackPressureValve valve = new ThresholdBackPressureValve(1);

    valve.notifyAsyncOperationRegistered();
    valve.notifyAsyncOperationCompleted(TestUtils.FUNCTION_1_ADDR);

    assertFalse(valve.shouldBackPressure());
  }

  @Test
  public void blockAddressTriggerBackpressure() {
    ThresholdBackPressureValve valve = new ThresholdBackPressureValve(500);

    valve.blockAddress(TestUtils.FUNCTION_1_ADDR);

    assertTrue(valve.shouldBackPressure());
  }

  @Test
  public void blockingAndUnblockingAddress() {
    ThresholdBackPressureValve valve = new ThresholdBackPressureValve(500);

    valve.blockAddress(TestUtils.FUNCTION_1_ADDR);
    valve.notifyAsyncOperationCompleted(TestUtils.FUNCTION_1_ADDR);

    assertFalse(valve.shouldBackPressure());
  }

  @Test
  public void unblockingDifferentAddressStillBackpressures() {
    ThresholdBackPressureValve valve = new ThresholdBackPressureValve(500);

    valve.blockAddress(TestUtils.FUNCTION_1_ADDR);
    valve.notifyAsyncOperationCompleted(TestUtils.FUNCTION_2_ADDR);

    assertTrue(valve.shouldBackPressure());
  }

  @Test
  public void blockTwoAddress() {
    ThresholdBackPressureValve valve = new ThresholdBackPressureValve(500);

    valve.blockAddress(TestUtils.FUNCTION_1_ADDR);
    valve.blockAddress(TestUtils.FUNCTION_2_ADDR);
    assertTrue(valve.shouldBackPressure());

    valve.notifyAsyncOperationCompleted(TestUtils.FUNCTION_1_ADDR);
    assertTrue(valve.shouldBackPressure());

    valve.notifyAsyncOperationCompleted(TestUtils.FUNCTION_2_ADDR);
    assertFalse(valve.shouldBackPressure());
  }
}
