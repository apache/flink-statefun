#!/bin/sh
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

cd $(dirname $0)

rm -rf ../../docs/downloads/walkthrough.zip

rm -rf statefun-walkthrough
cp -r ../../statefun-examples/statefun-python-greeter statefun-walkthrough

rm statefun-walkthrough/build-example.sh
rm statefun-walkthrough/pom.xml
rm -rf statefun-walkthrough/target

rm statefun-walkthrough/greeter/greeter.py
cp greeter.py statefun-walkthrough/greeter/greeter.py

rm statefun-walkthrough/README.md

sed '/apache_flink_statefun/d' statefun-walkthrough/greeter/Dockerfile > Dockerfile
mv Dockerfile statefun-walkthrough/greeter/Dockerfile

zip -r walkthrough.zip statefun-walkthrough
rm -rf statefun-walkthrough
mv walkthrough.zip ../../docs/downloads/walkthrough.zip
