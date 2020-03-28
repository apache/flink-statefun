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

##
## Variables with defaults (if not overwritten by environment)
##
MVN=${MVN:-mvn}

##
## Required variables
##
OLD_VERSION=${OLD_VERSION}
NEW_VERSION=${NEW_VERSION}

if [ -z "${OLD_VERSION}" ]; then
    echo "NEW_VERSION was not set."
    exit 1
fi

if [ -z "${NEW_VERSION}" ]; then
    echo "NEW_VERSION was not set."
    exit 1
fi

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should aways exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

###########################

cd ${PROJECT_ROOT}

# change version in all pom files
mvn versions:set -DgenerateBackupPoms=false -DnewVersion=${NEW_VERSION}

# change version in Python SDK's setup.py file
perl -pi -e "s#version=\'$OLD_VERSION\'#version=\'$NEW_VERSION\'#" statefun-python-sdk/setup.py

# change version strings in README
perl -pi -e "s#<version>(.*)$OLD_VERSION(.*)</version>#<version>$NEW_VERSION</version>#" README.md
perl -pi -e "s#-DarchetypeVersion=$OLD_VERSION#-DarchetypeVersion=$NEW_VERSION#" README.md

# change version strings in tools directory
perl -pi -e "s#version: $OLD_VERSION#version: $NEW_VERSION#" tools/k8s/Chart.yaml

# change version strings in docs config
perl -pi -e "s#version: \"$OLD_VERSION\"#version: \"$NEW_VERSION\"#" docs/_config.yml
perl -pi -e "s#version_title: \"$OLD_VERSION\"#version_title: \"$NEW_VERSION\"#" docs/_config.yml

# change Stateful Functions image version tags in all Dockerfiles and image build script
find . -name 'Dockerfile*' -type f -exec perl -pi -e "s#FROM flink-statefun:$OLD_VERSION#FROM flink-statefun:$NEW_VERSION#" {} \;
perl -pi -e "s#VERSION_TAG=$OLD_VERSION#VERSION_TAG=$NEW_VERSION#" tools/docker/build-stateful-functions.sh

git commit -am "[release] Update version to ${NEW_VERSION}"

NEW_VERSION_COMMIT_HASH=`git rev-parse HEAD`

echo "Done. Created a new commit for the new version ${NEW_VERSION}, with hash ${NEW_VERSION_COMMIT_HASH}"
echo "If this is a new version to be released (or a candidate to be voted on), don't forget to create a signed release tag on GitHub and push the changes."
echo "e.g., git tag -s -m \"Apache Flink Stateful Functions, release 1.1 candidate #2\" release-1.1-rc2 ${NEW_VERSION_COMMIT_HASH}"

cd ${CURR_DIR}