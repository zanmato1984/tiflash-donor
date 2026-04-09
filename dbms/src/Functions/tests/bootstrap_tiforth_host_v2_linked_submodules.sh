#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

git submodule update --init --recursive \
  contrib/abseil-cpp \
  contrib/aws \
  contrib/aws-c-auth \
  contrib/aws-c-cal \
  contrib/aws-c-common \
  contrib/aws-c-compression \
  contrib/aws-c-event-stream \
  contrib/aws-c-http \
  contrib/aws-c-io \
  contrib/aws-c-mqtt \
  contrib/aws-c-s3 \
  contrib/aws-c-sdkutils \
  contrib/aws-checksums \
  contrib/aws-crt-cpp \
  contrib/aws-s2n-tls \
  contrib/benchmark \
  contrib/boost \
  contrib/boringssl \
  contrib/cctz \
  contrib/client-c \
  contrib/double-conversion \
  contrib/fmtlib \
  contrib/googletest \
  contrib/grpc \
  contrib/highfive \
  contrib/jemalloc \
  contrib/kvproto \
  contrib/lz4 \
  contrib/magic_enum \
  contrib/poco \
  contrib/prometheus-cpp \
  contrib/protobuf \
  contrib/re2 \
  contrib/simdjson \
  contrib/simsimd \
  contrib/tiflash-proxy \
  contrib/tipb \
  contrib/usearch \
  contrib/xxHash \
  contrib/zlib-ng \
  contrib/zstd
