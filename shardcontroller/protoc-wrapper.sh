#!/bin/bash
# Wrapper script for protoc that reports a version compatible with protobuf-build 0.14.1
# The old crate expects major version == 3, but modern protoc uses 28+/35+ versioning.
# This wrapper intercepts --version to report 3.21.0, passing all else to real protoc.

REAL_PROTOC="/opt/homebrew/bin/protoc"

if [ "$1" = "--version" ]; then
  echo "libprotoc 3.21.0"
else
  exec "$REAL_PROTOC" "$@"
fi
