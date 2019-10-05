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
FROM python:3

RUN apt-get update && apt-get -y install texlive-latex-recommended texlive-fonts-recommended texlive-latex-extra latexmk

COPY requirements.txt /requirements.txt

RUN set -e; \
  pip install --no-cache-dir -r /requirements.txt

VOLUME "/build"
EXPOSE 8000
CMD ["make" "-C", "/build", "html"]
