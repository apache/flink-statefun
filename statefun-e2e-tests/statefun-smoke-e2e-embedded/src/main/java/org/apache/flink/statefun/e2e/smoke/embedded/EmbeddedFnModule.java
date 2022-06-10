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
package org.apache.flink.statefun.e2e.smoke.embedded;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.e2e.smoke.SmokeRunnerParameters;
import org.apache.flink.statefun.e2e.smoke.driver.Constants;
import org.apache.flink.statefun.e2e.smoke.driver.Ids;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(StatefulFunctionModule.class)
public class EmbeddedFnModule implements StatefulFunctionModule {
  public static final Logger LOG = LoggerFactory.getLogger(EmbeddedFnModule.class);

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    SmokeRunnerParameters parameters = SmokeRunnerParameters.from(globalConfiguration);
    LOG.info(parameters.toString());

    Ids ids = new Ids(parameters.getNumberOfFunctionInstances());

    FunctionProvider provider = new FunctionProvider(ids);
    binder.bindFunctionProvider(Constants.FN_TYPE, provider);
  }
}
