.. Licensed to the Apache Software Foundation (ASF) under one
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

##############
Configurations
##############

Stateful Functions includes a small number of SDK specific configurations.

Flink Configuration Options
^^^^^^^^^^^^^^^^^^^^^^^^^^^

These may be set through your job's ``flink-conf.yaml``.

+-----------------------------------------------------+-----------------------------------------------------+----------------------------+-----------------------------+
| Configuration                                       | Value                                               | Default                    | Options                     |
+=====================================================+=====================================================+============================+=============================+
| Adds the given key/value pair to the Stateful       | statefun.module.config.<KEY>                        | (none)                     |                             |
| Functions global configuration.                     |                                                     |                            |                             |
+------------------------+----------------------------+-----------------------------------------------------+----------------------------+-----------------------------+
| The serializer to use for on the wire messages.     | statefun.message.serializer                         | ``WITH_PROTOBUF_PAYLOADS`` | - ``WITH_PROTOBUF_PAYLOADS``|
|                                                     |                                                     |                            + - ``WITH_KRYO_PAYLOADS``    |
|                                                     |                                                     |                            + - ``WITH_RAW_PAYLOADS``     |
+------------------------+----------------------------+-----------------------------------------------------+----------------------------+-----------------------------+
| The name to display in the Flink-UI.                | statefun.flink-job-name                             | StatefulFunctions          |                             |
+------------------------+----------------------------+-----------------------------------------------------+----------------------------+-----------------------------+
| The number of bytes to use for in memory buffering  |                                                     |                            |                             |
| of the feedback channel, before spilling to disk.   | statefun.feedback.memory.size                       | 32 MB                      |                             |
+------------------------+----------------------------+-----------------------------------------------------+----------------------------+-----------------------------+
