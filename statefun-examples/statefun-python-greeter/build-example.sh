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
rm -f apache_flink_statefun-*-py3-none-any.whl
rm -rf __pycache__

# copy the whl distribution
cp ../../statefun-python-sdk/dist/apache_flink_statefun-*-py3-none-any.whl greeter/apache_flink_statefun-snapshot-py3-none-any.whl 2>/dev/null
rc=$?
if [[ ${rc} -ne 0 ]]; then
    echo "Failed copying the whl distribution, please build the Python distribution first."
    echo "To build the distribution:"
    echo "  goto to statefun-python-sdk/"
    echo "  call ./build-distribution.sh"
    exit 1;
fi

# build

docker-compose build

rm -f greeter/apache_flink_statefun-*-py3-none-any.whl

echo "Done. To start the example run: docker-compose up"

