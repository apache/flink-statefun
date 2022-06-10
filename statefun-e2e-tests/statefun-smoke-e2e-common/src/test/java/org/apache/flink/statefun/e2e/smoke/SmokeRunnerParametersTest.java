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

package org.apache.flink.statefun.e2e.smoke;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.Map;
import org.junit.Test;

public class SmokeRunnerParametersTest {

  @Test
  public void exampleUsage() {
    Map<String, String> keys = Collections.singletonMap("messageCount", "1");
    SmokeRunnerParameters parameters = SmokeRunnerParameters.from(keys);

    assertThat(parameters.getMessageCount(), is(1));
  }

  @Test
  public void roundTrip() {
    SmokeRunnerParameters original = new SmokeRunnerParameters();
    original.setCommandDepth(1234);

    SmokeRunnerParameters deserialized = SmokeRunnerParameters.from(original.asMap());

    assertThat(deserialized.getCommandDepth(), is(1234));
  }
}
