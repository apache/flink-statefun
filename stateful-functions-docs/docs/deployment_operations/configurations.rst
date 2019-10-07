.. Copyright 2019 Ververica GmbH.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
   
        http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

##############
Configurations
##############

Stateful Functions includes a small number of SDK specific configurations.

Command Line Arguments
^^^^^^^^^^^^^^^^^^^^^^

The following may be set as flags in the form ``--key value``:

+-----------------------------------------------------+-----------------------------------------------------+----------------------------+-----------------------------+
| Configuration                                       | Value                                               | Default                    | Options                     |
+=====================================================+=====================================================+============================+=============================+
| Flink checkpoint interval in milliseconds           | stateful-functions.state.checkpointing-interval-ms  | 30s                        | - (-1 to disable)           |
+------------------------+----------------------------+-----------------------------------------------------+----------------------------+-----------------------------+
| The serializer to use for on the wire messages.     | stateful-functions.message.serializer               | ``WITH_PROTOBUF_PAYLOADS`` | - ``WITH_PROTOBUF_PAYLOADS``|
|                                                     |                                                     |                            + - ``WITH_KRYO_PAYLOADS``    |
|                                                     |                                                     |                            + - ``WITH_RAW_PAYLOADS``     |
+------------------------+----------------------------+-----------------------------------------------------+----------------------------+-----------------------------+
| The name to display in the Flink-UI.                | stateful-functions.flink-job-name                   | StatefulFunctions          |                             |
+------------------------+----------------------------+-----------------------------------------------------+----------------------------+-----------------------------+

Flink Configuration Options
^^^^^^^^^^^^^^^^^^^^^^^^^^^

These may be set through your job's ``flink-conf.yaml``.

+-----------------------------------------------------+-----------------------------------------------------+----------------------------+
| Configuration                                       | Value                                               | Default                    |
+=====================================================+=====================================================+============================+
| The number of bytes to use for in memory buffering  |                                                     |                            |
| of the feedback channel, before spilling to disk.   | stateful-functions.feedback.memory.bytes            | 32 MB                      |
+------------------------+----------------------------+-----------------------------------------------------+----------------------------+
| Use a single MapState to multiplex different        |                                                     |                            |
| function types and persisted values instead of using|                                                     |                            |
| a ValueState for each <FunctionType, PersistedValue>|                                                     |                            |
| combination.                                        | stateful-functions.state.multiplex-flink-state      | true                       |
+------------------------+----------------------------+-----------------------------------------------------+----------------------------+

.. note::

    When using RocksDB each registered state is backed by a column family.
    By default column family's require 2x64 MB of state.
    This could prevent loading many functions with a small resource footprint.

