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
            <td><h5>out-local</h5></td>
            <td>Function</td>
            <td>The number of messages sent to a function on the same task slot.</td>
            <td>Counter</td>
        </tr>
        <tr>
            <td><h5>out-localRate</h5></td>
            <td>Function</td>
            <td>The average number of messages sent to a function on the same task slot per second.</td>
            <td>Meter</td>
        </tr>
        <tr>
            <td><h5>out-remote</h5></td>
            <td>Function</td>
            <td>The number of messages sent to a function on a different task slot.</td>
            <td>Counter</td>
        </tr>
        <tr>
            <td><h5>out-remoteRate</h5></td>
            <td>Function</td>
            <td>The average number of messages sent to a function on a different task slot per second.</td>
            <td>Meter</td>
        </tr>
        <tr>
            <td><h5>out-egress</h5></td>
            <td>Function</td>
            <td>The number of messages sent to an egress.</td>
            <td>Counter</td>
        </tr>
        <tr>
            <td><h5>feedback.produced</h5></td>
            <td>Operator</td>
            <td>The number of messages read from the feedback channel.</td>
            <td>Meter</td>
        </tr>
        <tr>
            <td><h5>feedback.producedRate</h5></td>
            <td>Operator</td>
            <td>The average number of messages read from the feedback channel per second.</td>
            <td>Meter</td>
        </tr>
    </tbody>
</table>