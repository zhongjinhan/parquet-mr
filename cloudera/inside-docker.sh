#!/usr/bin/env bash

. /opt/toolchain/toolchain.sh
export THRIFT_VERSION="0.9.3"
export THRIFT_HOME="/opt/toolchain/thrift-${THRIFT_VERSION}"
export PROTOC_HOME="/opt/toolchain/protobuf-2.5.0"
export PATH="${THRIFT_HOME}/bin:${PROTOC_HOME}/bin:$PATH"

if [[ ! -d "$THRIFT_HOME" ]]; then
    echo "THRIFT HOME ($THRIFT_HOME) does not exist!"
    exit 1
fi

if [[ ! -d "$PROTOC_HOME" ]]; then
    echo "PROTOC HOME ($PROTOC_HOME) does not exist!"
    exit 1
fi

mvn clean test --fail-at-end
