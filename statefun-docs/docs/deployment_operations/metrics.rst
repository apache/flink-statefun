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

#######
Metrics
#######

Stateful Functions includes a number of SDK specific metrics, scoped on a per-function basis, one level below operator scope.

+------------------------+-----------------------------------------------------+-----------------------------------------------------+
| Metric                 | Description                                         | Syntax                                              |
+========================+=====================================================+=====================================================+
| in                     | The number of incoming messages.                    | <function_namespace>.<function_name>.in             |
+------------------------+-----------------------------------------------------+-----------------------------------------------------+
| inRate                 | The average number of incoming messages per second. | <function_namespace>.<function_name>.inRate         |
+------------------------+-----------------------------------------------------+-----------------------------------------------------+
| out-local              | The number of messages sent to a function on the    |                                                     |
|                        + same task slot.                                     | <function_namespace>.<function_name>.out-local      |
+------------------------+-----------------------------------------------------+-----------------------------------------------------+
| out-localRate          | The average number of messages sent to a function   |                                                     |
|                        + on the same task slot per second.                   | <function_namespace>.<function_name>.out-localRate  |
+------------------------+-----------------------------------------------------+-----------------------------------------------------+
| out-remote             | The number of messages sent to a function on another|                                                     |
|                        + same task slot.                                     | <function_namespace>.<function_name>.out-remote     |
+------------------------+-----------------------------------------------------+-----------------------------------------------------+
| out-remoteRate         | The average number of messages sent to a function   |                                                     |
|                        + on another task slot per second.                    | <function_namespace>.<function_name>.out-remoteRate |
+------------------------+-----------------------------------------------------+-----------------------------------------------------+
| out-egress             | The number of messages sent to an egress.           | <function_namespace>.<function_name>.out-egress     |
+------------------------+-----------------------------------------------------+-----------------------------------------------------+
| out-egressRate         | The average number of messages sent to an egress    |                                                     |
|                        + per second.                                         | <function_namespace>.<function_name>.out-egressRate |
+------------------------+-----------------------------------------------------+-----------------------------------------------------+
| feedback.produced      | The number of messages read from the feedback       |                                                     |
|                        + channel.                                            | <operator>.feedback.produced                        |
+------------------------+-----------------------------------------------------+-----------------------------------------------------+
| feedback.producedRate  | The average number of messages read from the        |                                                     |
|                        + feedback channel per second.                        | <operator>.feedback.producedRate                    |
+------------------------+-----------------------------------------------------+-----------------------------------------------------+