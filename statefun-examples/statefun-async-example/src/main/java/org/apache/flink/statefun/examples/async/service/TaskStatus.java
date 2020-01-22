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
package org.apache.flink.statefun.examples.async.service;

/**
 * Represents A task status, as returned from the Dummy task service.
 *
 * <p>A Task might be either completed or uncompleted. If a task is completed then it would also
 * have a completion time.
 */
public class TaskStatus {
  private final String taskId;
  private final boolean completed;
  private final Long completionTime;

  TaskStatus(String taskId, boolean completed, Long completionTime) {
    this.taskId = taskId;
    this.completed = completed;
    this.completionTime = completionTime;
  }

  public String getTaskId() {
    return taskId;
  }

  public boolean isCompleted() {
    return completed;
  }

  public Long getCompletionTime() {
    return completionTime;
  }
}
