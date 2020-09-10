---
title: Metrics
nav-id: metrics
nav-pos: 3
nav-title: Metrics
nav-parent_id: deployment-and-ops
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

Stateful Functions includes a number of SDK specific metrics.
Along with the [standard metric scopes](https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/metrics.html#system-scope), Stateful Functions supports ``Function Scope`` which one level below operator scope.

``metrics.scope.function``
* Default: &lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;operator_name&gt;.&lt;subtask_index&gt;.&lt;function_namespace&gt;.&lt;function_name&gt;
* Applied to all metrics that were scoped to a function.


<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Metrics</th>
            <th class="text-left" style="width: 15%">Scope</th>
            <th class="text-left" style="width: 15%">Description</th>
            <th class="text-left" style="width: 10%">Type</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>in</h5></td>
            <td>Function</td>
            <td>The number of incoming messages.</td>
            <td>Counter</td>
        </tr>
        <tr>
            <td><h5>inRate</h5></td>
            <td>Function</td>
            <td>The average number of incoming messages per second.</td>
            <td>Meter</td>
        </tr>
        <tr>
            <td><h5>outLocal</h5></td>
            <td>Function</td>
            <td>The number of messages sent to a function on the same task slot.</td>
            <td>Counter</td>
        </tr>
        <tr>
            <td><h5>outLocalRate</h5></td>
            <td>Function</td>
            <td>The average number of messages sent to a function on the same task slot per second.</td>
            <td>Meter</td>
        </tr>
        <tr>
            <td><h5>outRemote</h5></td>
            <td>Function</td>
            <td>The number of messages sent to a function on a different task slot.</td>
            <td>Counter</td>
        </tr>
        <tr>
            <td><h5>outRemoteRate</h5></td>
            <td>Function</td>
            <td>The average number of messages sent to a function on a different task slot per second.</td>
            <td>Meter</td>
        </tr>
        <tr>
            <td><h5>outEgress</h5></td>
            <td>Function</td>
            <td>The number of messages sent to an egress.</td>
            <td>Counter</td>
        </tr>
       <tr>
            <td><h5>inflightAsyncOps</h5></td>
            <td>Function</td>
            <td>The number of uncompleted asynchronous operations.</td>
            <td>Counter</td>
        </tr>
        <tr>
            <td><h5>numBackLog</h5></td>
            <td>Remote Function</td>
            <td>The number of pending messages to be sent.</td>
            <td>Counter</td>
        </tr> 
        <tr>
           <td><h5>numBlockedAddress</h5></td>
           <td>Remote Function</td>
           <td>The number of addresses that are currently under back pressure.</td>
           <td>Counter</td>
        </tr>
        <tr>
            <td><h5>remoteInvocationFailures</h5></td>
            <td>Remote Function</td>
            <td>The number of failed attempts to invoke a function remotely.</td>
            <td>Counter</td>
         </tr>
         <tr>
            <td><h5>remoteInvocationFailuresRate</h5></td>
            <td>Remote Function</td>
            <td>The average number of failed attempts to invoke a function remotely.</td>
            <td>Meter</td>
         </tr>
         <tr>
            <td><h5>remoteInvocationLatency</h5></td>
            <td>Remote Function</td>
            <td>A distribution of remote function invocation latencies.</td>
            <td>Histogram</td>
        </tr>
        <tr>
            <td><h5>feedback.produced</h5></td>
            <td>Operator</td>
            <td>The number of messages read from the feedback channel.</td>
            <td>Counter</td>
        </tr>
        <tr>
            <td><h5>feedback.producedRate</h5></td>
            <td>Operator</td>
            <td>The average number of messages read from the feedback channel per second.</td>
            <td>Meter</td>
        </tr>
        <tr>
            <td><h5>inflightAsyncOps</h5></td>
            <td>Operator</td>
            <td>The total number of uncompleted asynchronous operations (across all function types).</td>
            <td>Counter</td>
        </tr>
    </tbody>
</table>