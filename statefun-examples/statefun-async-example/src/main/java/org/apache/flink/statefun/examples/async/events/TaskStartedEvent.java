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
package org.apache.flink.statefun.examples.async.events;

/** A message represents an event of a new task was created. A task has an id and a startingTime. */
public final class TaskStartedEvent {
  private final String taskId;
  private final Long startTime;

  public TaskStartedEvent(String taskId, Long startTime) {
    this.taskId = taskId;
    this.startTime = startTime;
  }

  public String getTaskId() {
    return taskId;
  }

  public Long getStartTime() {
    return startTime;
  }
}
