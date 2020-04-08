#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Deploys snapshot builds to Apache's snapshot repository.
#

# fail immediately
set -o errexit
set -o nounset

#
# Variables with defaults (if not overwritten by environment)
#
MVN=${MVN:-mvn}

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should aways exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

###########################

cd "$PROJECT_ROOT"

CURRENT_STATEFUN_VERSION=`${MVN} org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -E '^([0-9]+.[0-9]+(.[0-9]+)?(-[a-zA-Z0-9]+)?)$'`
echo "Detected current version as: '$CURRENT_STATEFUN_VERSION'"

if [[ ${CURRENT_STATEFUN_VERSION} == *SNAPSHOT* ]] ; then
    echo "Deploying to repository.apache.org/content/repositories/snapshots/"
    ${MVN} clean deploy -Papache-release -Dgpg.skip -Drat.skip=true -Drat.ignoreErrors=true -DskipTests -DretryFailedDeploymentCount=10
    exit 0
else
    echo "Snapshot deployments should only be done for snapshot versions"
    exit 1
fi
