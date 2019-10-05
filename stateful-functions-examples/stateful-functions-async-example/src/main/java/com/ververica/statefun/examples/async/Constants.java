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

import com.ververica.statefun.examples.async.events.TaskCompletionEvent;
import com.ververica.statefun.examples.async.events.TaskStartedEvent;
import com.ververica.statefun.sdk.io.EgressIdentifier;
import com.ververica.statefun.sdk.io.IngressIdentifier;

final class Constants {

  static final IngressIdentifier<TaskStartedEvent> REQUEST_INGRESS =
      new IngressIdentifier<>(
          TaskStartedEvent.class, "com.ververica.statefun.examples.async", "tasks");

  static final EgressIdentifier<TaskCompletionEvent> RESULT_EGRESS =
      new EgressIdentifier<>(
          "com.ververica.statefun.examples.async", "out", TaskCompletionEvent.class);
}
