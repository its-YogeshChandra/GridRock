#!/bin/bash
# Wrapper around protoc that spoofs the version string.
# protobuf-build (used by raft-proto) requires protoc 3.x,
# but Homebrew installs protoc 35.x which uses a new versioning scheme.
# protoc 35.1 is fully wire-compatible with 3.x; only the version number changed.

if [ "$1" = "--version" ]; then
    echo "libprotoc 3.21.0"
else
    exec /opt/homebrew/bin/protoc "$@"
fi
