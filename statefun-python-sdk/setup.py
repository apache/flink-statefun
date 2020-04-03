################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from setuptools import setup

import io
import os

this_directory = os.path.abspath(os.path.dirname(__file__))
with io.open(os.path.join(this_directory, 'README.md'), 'r', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='apache-flink-statefun',
    version='2.0.0',
    packages=["statefun"],
    url='https://github.com/apache/flink-statefun',
    license='https://www.apache.org/licenses/LICENSE-2.0',
    license_files=["LICENSE", "NOTICE"],
    author='Apache Software Foundation',
    author_email='dev@flink.apache.org',
    description='Python SDK for Apache Flink Stateful functions',
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=['protobuf>=3.11.3,<4.0.0'],
    tests_require=['pytest'],
    python_requires='>=3.5',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7']
)
