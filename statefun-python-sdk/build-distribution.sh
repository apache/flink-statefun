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

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
SDK_PROTOS_DIR="${BASE_DIR}/../statefun-sdk-protos/src/main/protobuf"
IN_DOCKER_BUILD_COMMAND="
apk add protobuf-dev && \
protoc *proto --python_out=statefun/ && \
python3 setup.py sdist bdist_wheel
"


###########################

cd ${BASE_DIR}

# create a target/ directory like in MAVEN.
# this directory will contain a temporary copy of the source and
# addtional generated sources (for example sdk .proto files copied from else where)
# eventually the built artifact will be copied to ${BASE_DIR}/dist and this target
# directory will be deleted.
rm -fr dist/
rm -fr target/
mkdir -p target/

# copy all the sources into target
rsync -a --exclude=target * target/

# copy the additional .proto files from the SDK
find ${SDK_PROTOS_DIR} -type f -name "*proto" -exec cp {} target/ \;

cd target/

# built the Python SDK inside a Docker container.
# This build step also generates Protobuf files.
docker run -v "${BASE_DIR}/target:/app" \
	--rm \
	--workdir /app \
	-i python:3.7-alpine /bin/sh -c "${IN_DOCKER_BUILD_COMMAND}"

cp -r "dist/" "${BASE_DIR}/dist"

echo "Built Python SDK wheels and packages at ${BASE_DIR}/dist."

cd ${CURR_DIR}
