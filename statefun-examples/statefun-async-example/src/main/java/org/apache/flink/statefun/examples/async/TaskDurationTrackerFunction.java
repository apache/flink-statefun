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
package org.apache.flink.statefun.examples.async;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.statefun.examples.async.events.TaskCompletionEvent;
import org.apache.flink.statefun.examples.async.events.TaskStartedEvent;
import org.apache.flink.statefun.examples.async.service.TaskQueryService;
import org.apache.flink.statefun.examples.async.service.TaskStatus;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;

/**
 * TaskDurationTrackerFunction - demonstrates the use of asynchronous operations.
 *
 * <p>In this example scenario there are tasks executing in an an external system, the external
 * system exposes a {@link TaskQueryService} that can report back the status of each individual
 * task, in addition the external system generates a {@link TaskStartedEvent} with the task id and
 * creation time.
 *
 * <p>This function reacts to {@link TaskStartedEvent} and pulls the external {@link
 * TaskQueryService} for the task completion, when finally the task completes, this function
 * produces an {@link TaskCompletionEvent} with the task execution duration.
 */
final class TaskDurationTrackerFunction implements StatefulFunction {

  static final FunctionType TYPE =
      new FunctionType("org.apache.flink.statefun.examples.async", "duration-tracker");

  private final TaskQueryService service;

  TaskDurationTrackerFunction(TaskQueryService service) {
    this.service = Objects.requireNonNull(service);
  }

  @Override
  public void invoke(Context context, Object message) {
    if (message instanceof TaskStartedEvent) {
      // We received a TaskStartedEvent, in response, we need to check with an external service what
      // is the status of that task. Since the external service (represented by the client) is
      // asynchronous (returns a CompletableFuture) we register that future as a pending
      // asynchronous operation, and we will
      // get notified once the async operation completes via the special AsyncOperationResult
      // message.
      // we also attach the original input message as metadata to the async operation.
      TaskStartedEvent e = (TaskStartedEvent) message;
      CompletableFuture<TaskStatus> result = service.getTaskStatusAsync(e.getTaskId());
      context.registerAsyncOperation(message, result);
      return;
    }
    if (message instanceof AsyncOperationResult) {
      // This is a result of an async operation we have previously registered.
      // The message's metadata would be the original input event that triggered the async operation
      // (TaskStartedEvent)
      // and possibly the asynchronously computed TaskStatus.
      @SuppressWarnings("unchecked")
      AsyncOperationResult<TaskStartedEvent, TaskStatus> asyncOp =
          (AsyncOperationResult<TaskStartedEvent, TaskStatus>) message;

      onAsyncOperationResultEvent(context, asyncOp);
      return;
    }
    throw new IllegalArgumentException("Unknown event " + message);
  }

  /**
   * Handle the result of an asynchronous operation. The logic of this example is as follows: 1. If
   * the async operation itself failed (i.e. IOException, TimeoutException etc') just blindly retry
   * within a second 2. If the
   */
  private static void onAsyncOperationResultEvent(
      Context context, AsyncOperationResult<TaskStartedEvent, TaskStatus> asyncOp) {

    // We have attached the original TaskStartedEvent as a metadata, when registering the async
    // operation, so we can just grab it.
    final TaskStartedEvent e = asyncOp.metadata();

    if (!asyncOp.successful()) {
      // Something went wrong while trying to obtain the TaskStatus asynchronously, we can inspect
      // the cause by
      // calling asyncOp.throwable() or asking if the status is unknown (asyncOp.unknown())
      // in any case we retry in 1 second, by just sending a delayed message to ourselves.
      Duration delay = oneSecondPlusJitter();
      context.sendAfter(delay, context.self(), e);
      return;
    }
    // The async op has completed successfully now we can obtain the asynchronously computed value.
    final TaskStatus status = asyncOp.value();
    if (!status.isCompleted()) {
      // The task status is not yet complete, therefore we need to pull the status again at some
      // later point in time, lets retry in 10 seconds
      context.sendAfter(Duration.ofSeconds(10), context.self(), e);
      return;
    }
    handleCompletedTask(context, e, status);
  }

  /**
   * compute a duration that represents slightly more than one second (with a random jitter) to
   * avoid thundering herds.
   */
  private static Duration oneSecondPlusJitter() {
    final long randomJitter = ThreadLocalRandom.current().nextLong(1_000, 1_250);
    return Duration.ofMillis(randomJitter);
  }

  /** The task was completed, we can compute the task execution duration and emit it downstream. */
  private static void handleCompletedTask(
      Context context, TaskStartedEvent taskStartedEvent, TaskStatus finishedTaskStatus) {

    TaskCompletionEvent taskCompletionEvent =
        new TaskCompletionEvent(
            taskStartedEvent.getTaskId(),
            taskStartedEvent.getStartTime(),
            finishedTaskStatus.getCompletionTime());

    context.send(Constants.RESULT_EGRESS, taskCompletionEvent);
  }
}
