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

package com.ververica.statefun.examples.async;

import com.google.auto.service.AutoService;
import com.ververica.statefun.examples.async.service.DummyTaskQueryService;
import com.ververica.statefun.examples.async.service.TaskQueryService;
import com.ververica.statefun.sdk.spi.StatefulFunctionModule;
import java.util.Map;

@AutoService(StatefulFunctionModule.class)
public class Module implements StatefulFunctionModule {

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    TaskQueryService service = new DummyTaskQueryService();

    binder.bindFunctionProvider(
        TaskDurationTrackerFunction.TYPE, unused -> new TaskDurationTrackerFunction(service));

    binder.bindIngressRouter(
        Constants.REQUEST_INGRESS,
        (message, downstream) ->
            downstream.forward(TaskDurationTrackerFunction.TYPE, message.getTaskId(), message));
  }
}
