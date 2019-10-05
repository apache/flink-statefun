#
# Copyright 2019 Ververica GmbH.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM flink:1.9.0

ENV ROLE worker
ENV MASTER_HOST localhost
ENV STATEFUL_FUNCTIONS_HOME /opt/stateful-functions
ENV STATEFUL_FUNCTIONS_MODULES $STATEFUL_FUNCTIONS_HOME/modules

# cleanup flink-lib
RUN rm -fr $FLINK_HOME/lib/flink-table*jar

# copy our distriubtion template
COPY flink/ $FLINK_HOME/

# add user modules
USER root

RUN mkdir -p $STATEFUL_FUNCTIONS_MODULES && \
    useradd --system --home-dir $STATEFUL_FUNCTIONS_HOME --uid=9998 --gid=flink stateful-functions && \
    chown -R stateful-functions:flink $STATEFUL_FUNCTIONS_HOME && \
    chmod -R g+rw $STATEFUL_FUNCTIONS_HOME

# entry point 
ADD docker-entry-point.sh /docker-entry-point.sh

ENTRYPOINT ["/docker-entry-point.sh"]
