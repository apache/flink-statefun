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

# The following section installs Flink 1.10-SNAPSHOT (nightly)
# Once Flink 1.10 would be released we will remove that section.

RUN rm -rf /opt/flink && \
  mkdir -p /opt/flink && \
  cd /opt/flink && \
  wget --quiet https://s3.amazonaws.com/flink-nightly/flink-1.10-SNAPSHOT-bin-hadoop2.tgz && \
  tar xzf flink-1.10-SNAPSHOT-bin-hadoop2.tgz --strip-components 1 && \
  rm -f flink-1.10-SNAPSHOT-bin-hadoop2.tgz && \
  chown -R flink:flink /opt/flink

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
