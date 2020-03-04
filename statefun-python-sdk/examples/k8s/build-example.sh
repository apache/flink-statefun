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

# clean
rm -f statefun-1.1.0-py3-none-any.whl
rm -rf __pycache__

# this part would be removed, once statefun will be released to PyPI
cp ../../dist/statefun-1.1.0-py3-none-any.whl .

# build the flask container
docker build -f Dockerfile.python-worker . -t k8s-demo-python-worker

# this part would be removed, once statefun will be released to PyPI
rm -f statefun-1.1.0-py3-none-any.whl

# build the statefun Flink image
docker build -f Dockerfile.statefun . -t k8s-demo-statefun

