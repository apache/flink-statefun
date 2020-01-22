#!/bin/bash 

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#
# Role types
#
WORKER="worker"
MASTER="master"

#
# Environment
# 
FLINK_HOME=${FLINK_HOME:-"/opt/flink/bin"}
ROLE=${ROLE:-"worker"}
MASTER_HOST=${MASTER_HOST:-"localhost"}

#
# Start a service depending on the role.
#
if [[ "${ROLE}" == "${WORKER}" ]]; then
  #
  # start the TaskManager (worker role)
  #
  exec ${FLINK_HOME}/bin/taskmanager.sh start-foreground \
    -Djobmanager.rpc.address=${MASTER_HOST}

elif [[ "${ROLE}" == "${MASTER}" ]]; then
  #
  # start the JobManager (master role) with our predefined job.
  #
  exec $FLINK_HOME/bin/standalone-job.sh \
    start-foreground \
    -Djobmanager.rpc.address=${MASTER_HOST} \
   "$@"
else
  #
  # unknown role
  #
  echo "unknown role ${ROLE}"
  exit 1
fi
