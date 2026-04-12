# TiForth Host-v2 Linked Donor Adapter Tests

This note captures the compile/link-time setup used by the TiForth donor
adapter proving tests:

- `gtest_tiforth_execution_host_v2_cast.cpp`
- `gtest_tiforth_execution_host_v2_inner_hash_join.cpp`

Runtime symbol dispatch (`dlopen` / `dlsym`) is not used on this path.
These host-v2 proving tests are compiled into
`gtests_tiforth_execution_host_v2` only when
`-DENABLE_TIFORTH_HOST_V2_LINKED_TESTS=ON` is set.

## Bootstrap submodules (fresh donor worktree)

Before running the linked host-v2 configure/build commands in a fresh worktree,
initialize the required donor submodules (including nested dependencies used by
the linked proving binaries) with:

```bash
./dbms/src/Functions/tests/bootstrap_tiforth_host_v2_linked_submodules.sh
```

## Configure

Choose one linked-library input mode and keep the path absolute.
When linked tests are enabled, CMake also requires `CMAKE_NM` so host-v2
symbols can be preflighted before `gtests_tiforth_execution_host_v2` is wired.

### Mode A: direct library path

```bash
cmake -S . -B /tmp/tiflash-linked-host-v2 -GNinja \
  -DENABLE_TIFORTH_HOST_V2_LINKED_TESTS=ON \
  -DTIFORTH_FFI_C_LIBRARY=/abs/path/to/libtiforth_ffi_c.<so|dylib>
```

### Mode B: library directory + optional library name

```bash
cmake -S . -B /tmp/tiflash-linked-host-v2 -GNinja \
  -DENABLE_TIFORTH_HOST_V2_LINKED_TESTS=ON \
  -DTIFORTH_FFI_C_LIB_DIR=/abs/path/to/libdir \
  -DTIFORTH_FFI_C_LIB_NAME=tiforth_ffi_c
```

## Build

```bash
cmake --build /tmp/tiflash-linked-host-v2 --target gtests_tiforth_execution_host_v2
```

## Run proving tests

```bash
ctest --test-dir /tmp/tiflash-linked-host-v2 \
  -R TestTiforthExecutionHostV2LinkedProving \
  --output-on-failure
```

The CTest entry runs this exact proving-slice gtest filter:

```bash
/tmp/tiflash-linked-host-v2/dbms/gtests_tiforth_execution_host_v2 \
  --gtest_filter='TestTiforthExecutionHostV2Cast.CastUtf8ToDecimalParitySerialAndParallel:TestTiforthExecutionHostV2Cast.CastUtf8ToDecimalScaleLossWarningParitySerialAndParallel:TestTiforthExecutionHostV2InnerHashJoin.InnerHashJoinPayloadParitySerialAndParallel:TestTiforthExecutionHostV2InnerHashJoin.InnerHashJoinPayloadParityHighPartitionMaxBlockSerialAndParallel'
```
