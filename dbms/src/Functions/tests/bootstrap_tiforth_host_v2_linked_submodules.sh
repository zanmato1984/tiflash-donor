#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

git submodule update --init --recursive \
  contrib/abseil-cpp \
  contrib/aws \
  contrib/aws-c-common \
  contrib/aws-crt-cpp \
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
