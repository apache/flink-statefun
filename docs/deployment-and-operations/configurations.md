---
title: Configurations
nav-id: configurations
nav-pos: 2
nav-title: Configurations
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

Stateful Functions includes a small number of SDK specific configurations.
These may be set through your job's ``flink-conf.yaml``.

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Key</th>
            <th class="text-left" style="width: 15%">Default</th>
            <th class="text-left" style="width: 10%">Type</th>
            <th class="text-left" style="width: 55%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>statefun.module.global-config.&lt;KEY&gt;</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Adds the given key/value pair to the Stateful Functions global configuration.</td>
        </tr>
		<tr>
            <td><h5>statefun.message.serializer</h5></td>
            <td style="word-wrap: break-word;">WITH_PROTOBUF_PAYLOADS</td>
            <td>Message Serializer</td>
            <td>The serializer to use for on the wire messages. Options are WITH_PROTOBUF_PAYLOADS, WITH_KRYO_PAYLOADS, WITH_RAW_PAYLOADS.</td>
        </tr>
		<tr>
            <td><h5>statefun.flink-job-name</h5></td>
            <td style="word-wrap: break-word;">StatefulFunctions</td>
            <td>String</td>
            <td>The name to display in the Flink-UI.</td>
        </tr>
		<tr>
            <td><h5>statefun.feedback.memory.size</h5></td>
            <td style="word-wrap: break-word;">32 MB</td>
            <td>Memory</td>
            <td>TThe number of bytes to use for in memory buffering of the feedback channel, before spilling to disk.</td>
        </tr>
	</tbody>
</table>