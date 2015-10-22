#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# This file is sourced before running pre_commit_hook.sh and
# post_commit_hook.sh to allow setting up the environment in which those
# scripts run. To set an environment variable for those scripts, must export
# it.
#

export TOOLS_REPO_URI="git@github.mtv.cloudera.com:parquet/parquet.git"
export TOOLS_BRANCH="tools-cdh5-1.5.0"
export PROJECT_REPO_URI="git@github.mtv.cloudera.com:CDH/parquet.git"
export JAVA7_BUILD=true

pushd /opt/toolchain > /dev/null
source /opt/toolchain/toolchain.sh
popd > /dev/null

# setup thrift and protoc
THRIFT_HOME=/opt/toolchain/thrift-0.9.0
export PATH=$THRIFT_HOME/bin:/opt/toolchain/protobuf-2.5.0/bin:$PATH

