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

FROM flink-statefun:3.3.0

RUN mkdir -p /opt/statefun/modules/statefun-smoke-e2e
COPY statefun-smoke-e2e-driver.jar /opt/statefun/modules/statefun-smoke-e2e/
COPY remote-module/ /opt/statefun/modules/statefun-smoke-e2e/
COPY flink-conf.yaml $FLINK_HOME/conf/flink-conf.yaml

# Add the server's self seigned certificate to our trusted certificates.

ADD ssl/server.crt /tmp/server.crt
RUN keytool -importcert -file /tmp/server.crt -alias remote-function-host -keystore $JAVA_HOME/lib/security/cacerts -storepass changeit -noprompt