#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

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

function getVersion() {
    here="`dirname \"$0\"`" # relative
    here="`( cd \"${here}\" && pwd )`" # absolute and normalized
    if [ -z "$here" ] ; then
        # for some reason, the path is not accessible
        exit 1
    fi
    project_home="`dirname \"$here\"`"
    cd "$project_home"
	echo `${MVN} org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -E '^([0-9]+.[0-9]+(.[0-9]+)?(-[a-zA-Z0-9]+)?)$'`
}

CURRENT_STATEFUN_VERSION=`getVersion`

echo "Detected current version as: '$CURRENT_STATEFUN_VERSION'"

if [[ ${CURRENT_STATEFUN_VERSION} == *SNAPSHOT* ]] ; then
    echo "Deploying to repository.apache.org/content/repositories/snapshots/"
    ${MVN} clean deploy -Papache-release -Dgpg.skip -DskipTests -DretryFailedDeploymentCount=10
    exit 0
else
    echo "Snapshot deployments should only be done for snapshot versions"
    exit 1
fi
