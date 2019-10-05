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

#######
Metrics
#######

Stateful Functions includes a number of SDK specific metrics, scoped on a per function basis, one level below operator scope.

<function_namespace>.<function_name>.in     
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The number of incoming messages.

<function_namespace>.<function_name>.inRate   
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The average number of incoming messages per second. 

<function_namespace>.<function_name>.out-local
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The number of messages sent to a function on the same task slot.

<function_namespace>.<function_name>.out-localRate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The average number of messages sent to a function on the same task slot per second.

<function_namespace>.<function_name>.out-remote 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The number of messages sent to a function on another same task slot.

<function_namespace>.<function_name>.out-remoteRate 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The average number of messages sent to a function on another task slot per second.

<function_namespace>.<function_name>.out-egress 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The number of messages sent to an egress.

<function_namespace>.<function_name>.out-egressRate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The average number of messages sent to an egress per second.

<operator>.writeback.produced
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The number of messages read from the write back channel.

<operator>.writeback.produced
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The average number of messages read from the write back channel per second.