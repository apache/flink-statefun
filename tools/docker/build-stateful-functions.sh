#!/bin/bash
#
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

set -e

#
# setup the environment 
#
basedir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
project_root="${basedir}/../../" # ditch tools/docker
flink_template="${basedir}/flink-distribution-template"

#
# check if the artifacts were build
#
distribution_jar=$(find ${project_root} -type f -name "statefun-flink-distribution*jar" -not -name "*example*")
if [[ -z "${distribution_jar}" ]]; then
	echo "unable to find statefun-flink-distribution jar, please build the maven project first"
	exit 1
fi
core_jar=$(find ${project_root} -type f -name "statefun-flink-core*jar")
if [[ -z "${core_jar}" ]]; then
	echo "unable to find statefun-flink-core jar, please build the maven project first"
	exit 2 
fi

#
# create a scratch space for a minimal docker context
#
docker_context_root=`mktemp -d 2>/dev/null || mktemp -d -t 'statefun-docker-context'`
docker_context_flink="${docker_context_root}/flink"

#
# prepare the adjustments to the vanilla flink distribution
#
mkdir -p ${docker_context_flink}
cp -r ${flink_template}/* ${docker_context_flink}/
mkdir -p ${docker_context_flink}/lib
cp ${distribution_jar} ${docker_context_flink}/lib/statefun-flink-distribution.jar
cp ${core_jar} ${docker_context_flink}/lib/statefun-flink-core.jar
# build the docker image
cd ${docker_context_root}
cp ${basedir}/Dockerfile ${docker_context_root}
cp ${basedir}/docker-entry-point.sh ${docker_context_root}
docker build . -t statefun

# clean again
rm -rf ${docker_context_root}
