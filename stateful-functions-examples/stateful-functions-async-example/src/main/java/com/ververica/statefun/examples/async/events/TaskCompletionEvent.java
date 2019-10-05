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

package com.ververica.statefun.examples.async.events;

/** A message represents an event of a task completion. */
public final class TaskCompletionEvent {
  private final String taskId;
  private final Long startTime;
  private final Long endTime;

  public TaskCompletionEvent(String taskId, Long startTime, Long endTime) {
    this.taskId = taskId;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public String getTaskId() {
    return taskId;
  }

  public Long getStartTime() {
    return startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public long taskCompletionDuration() {
    return endTime - startTime;
  }
}
