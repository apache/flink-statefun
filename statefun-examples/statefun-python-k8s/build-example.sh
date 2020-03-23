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

PYTHON_IMAGE_NAME="k8s-demo-python-worker"
PYTHON_SERVICE_NAME="python-worker"
STATEFUN_IMAGE_NAME="k8s-demo-statefun"
PARALLELISM=3
K8S_RESOURCES_YAML="k8s-demo.yaml"

# clean
rm -f apache_flink_statefun-*-py3-none-any.whl
rm -rf __pycache__

# copy the whl distribution, it must be first built by calling build-distribution.sh 
cp ../../statefun-python-sdk/dist/apache_flink_statefun-*-py3-none-any.whl apache_flink_statefun-snapshot-py3-none-any.whl 2>/dev/null
rc=$?
if [[ ${rc} -ne 0 ]]; then
    echo "Failed copying the whl distribution, please build the distribution first by calling ./build-distribution.sh" 
    exit 1;
fi

# build the flask container
docker build -f Dockerfile.python-worker . -t ${PYTHON_IMAGE_NAME}

rm -f apache_flink_statefun-*-py3-none-any.whl

# build the statefun Flink image
docker build -f Dockerfile.statefun . -t ${STATEFUN_IMAGE_NAME}

helm template resources \
  --set worker.replicas=${PARALLELISM} \
  --set worker.image=${STATEFUN_IMAGE_NAME} \
  --set python.image=${PYTHON_IMAGE_NAME} \
  --set python.name=${PYTHON_SERVICE_NAME} > ${K8S_RESOURCES_YAML}


echo "Successfully created ${STATEFUN_IMAGE_NAME}, ${PYTHON_IMAGE_NAME} Docker images and ${K8S_RESOURCES_YAML}"
echo "Upload these Docker images to your docker registry that is accssible from K8S, and"
echo "Use: kubectl create -f ${K8S_RESOURCES_YAML}"

