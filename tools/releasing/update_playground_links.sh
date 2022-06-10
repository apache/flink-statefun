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
## Required variables
##
PREVIOUS_RELEASE_VERSION=${PREVIOUS_RELEASE_VERSION}
CURRENT_RELEASE_VERSION=${CURRENT_RELEASE_VERSION}

if [ -z "${PREVIOUS_RELEASE_VERSION}" ]; then
    echo "PREVIOUS_RELEASE_VERSION was not set."
    exit 1
fi

if [ -z "${CURRENT_RELEASE_VERSION}" ]; then
    echo "CURRENT_RELEASE_VERSION was not set."
    exit 1
fi

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should always exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

###########################

cd ${PROJECT_ROOT}

# change version strings in README
perl -pi -e "s#https://github.com/apache/flink-statefun-playground/tree/release-$PREVIOUS_RELEASE_VERSION#https://github.com/apache/flink-statefun-playground/tree/release-$CURRENT_RELEASE_VERSION#g" README.md statefun-sdk-python/README.md statefun-sdk-js/README.md
perl -pi -e "s#git clone -b release-$PREVIOUS_RELEASE_VERSION https://github.com/apache/flink-statefun-playground.git#git clone -b release-$CURRENT_RELEASE_VERSION https://github.com/apache/flink-statefun-playground.git#g" README.md

cd ${CURR_DIR}