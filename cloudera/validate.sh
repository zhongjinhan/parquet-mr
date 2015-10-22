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

function exists() {
  git ls-remote --heads "$1" | grep -q "$2" > /dev/null 2>&1
}

pushd `dirname $0` > /dev/null
here=`pwd`
popd > /dev/null 2>&1

source $here/env.sh

export GIT_SSH=$here/git-ssh.sh

if [ -z "$TOOLS_REPO_URI" ]; then
  echo "Failed to bootstrap commit-flow: \$TOOLS_REPO_URI must be set in env.sh" >&2
  exit 1
fi

if [ -n "$TOOLS_BRANCH" ]; then
  # use the tools branch from env.sh
  tools_branch=$TOOLS_BRANCH

  if ! exists $TOOLS_REPO_URI $TOOLS_BRANCH; then
    echo "Missing commit-flow tools branch: $TOOLS_BRANCH" >&2
    exit 1;
  fi

else
  project=$(basename `pwd`)

  # try to find a tools branch for this project
  current_branch=${GIT_BRANCH#*/}

  if echo $current_branch | grep -iq '_dev$'; then
    # strip off the _dev part
    base_branch=${current_branch%_*}
  else
    base_branch=$current_branch
  fi

  # cdh5-1.5.0_5.5.x => cdh5-1.5.0
  parent_branch=${base_branch%_*}

  # find the closest tools branch (tools-cdh5-1.5.0_5.5.x or tools-cdh5-1.5.0)
  if exists $TOOLS_REPO_URI "$project-$base_branch"; then
    tools_branch="$project-$base_branch"
  elif [ "$parent_ranch" != $base_branch ] && exists $TOOLS_REPO_URI "$project-$parent_branch"; then
    tools_branch="$project-$parent_branch"
  else
    echo "Missing commit-flow tools branch: $project-$parent_branch" >&2
    exit 1;
  fi
fi

if [ ! -d "$here/tools" ]; then
  # clone just the tools branch
  echo "Creating $here/tools for commit-flow tools"
  git init $here/tools
fi

# fetch just the tools branch, then check it out
echo "Checking out $tools_branch of $TOOLS_REPO_URI"
pushd $here/tools > /dev/null
git fetch $TOOLS_REPO_URI $tools_branch && git checkout FETCH_HEAD || (echo "Failed to checkout tools" && exit 1)
popd > /dev/null

exec bash cloudera/tools/validate.sh $@
