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
package org.apache.flink.statefun.flink.core.feedback;

import java.io.Serializable;
import java.util.Objects;

/** A FeedbackKey bounded to a subtask index. */
@SuppressWarnings("unused")
public final class SubtaskFeedbackKey<V> implements Serializable {

  private static final long serialVersionUID = 1;

  private final String pipelineName;
  private final int subtaskIndex;
  private final long invocationId;
  private final int attemptId;

  SubtaskFeedbackKey(String pipeline, long invocationId, int subtaskIndex, int attemptId) {
    this.pipelineName = Objects.requireNonNull(pipeline);
    this.invocationId = invocationId;
    this.subtaskIndex = subtaskIndex;
    this.attemptId = attemptId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SubtaskFeedbackKey<?> that = (SubtaskFeedbackKey<?>) o;
    return subtaskIndex == that.subtaskIndex
        && invocationId == that.invocationId
        && attemptId == that.attemptId
        && Objects.equals(pipelineName, that.pipelineName);
  }

  @Override
  public String toString(){
    return String.format("SubtaskFeedbackKey [pipelineName %s, subtaskIndex %d, invocationId %d, attempId %d]",
            pipelineName, subtaskIndex, invocationId, attemptId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipelineName, subtaskIndex, invocationId, attemptId);
  }
}
