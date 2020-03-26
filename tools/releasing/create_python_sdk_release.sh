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
SKIP_GPG=${SKIP_GPG:-false}
MVN=${MVN:-mvn}

##
## Required variables
##
RELEASE_VERSION=${RELEASE_VERSION}

if [ -z "${RELEASE_VERSION}" ]; then
    echo "RELEASE_VERSION was not set."
    exit 1
fi

# fail immediately
set -o errexit
set -o nounset

if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
else
    SHASUM="sha512sum"
fi

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should aways exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

###########################

RELEASE_DIR=${PROJECT_ROOT}/release/
mkdir -p ${RELEASE_DIR}

cd ${PROJECT_ROOT}/statefun-python-sdk/
./build-distribution.sh

mv dist/* ${RELEASE_DIR}

SOURCE_DIST="apache-flink-statefun-$RELEASE_VERSION.tar.gz"
WHL_VERSION=`echo "$RELEASE_VERSION" | tr - _`
WHL_DIST="apache_flink_statefun-$WHL_VERSION-py3-none-any.whl"

cd ${RELEASE_DIR}
# Sign sha the files
if [ "$SKIP_GPG" == "false" ] ; then
    gpg --armor --detach-sig ${SOURCE_DIST}
    gpg --armor --detach-sig ${WHL_DIST}
fi
${SHASUM} "${SOURCE_DIST}" > "${SOURCE_DIST}.sha512"
${SHASUM} "${WHL_DIST}" > "${WHL_DIST}.sha512"

echo "Created Python SDK distribution files at $RELEASE_DIR."

cd ${CURR_DIR}
