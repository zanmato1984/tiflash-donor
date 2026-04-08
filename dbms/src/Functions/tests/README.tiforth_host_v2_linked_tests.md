# TiForth Host-v2 Linked Donor Adapter Tests

This note captures the compile/link-time setup used by the TiForth donor
adapter proving tests:

- `gtest_tiforth_execution_host_v2_cast.cpp`
- `gtest_tiforth_execution_host_v2_inner_hash_join.cpp`

Runtime symbol dispatch (`dlopen` / `dlsym`) is not used on this path.

## Configure

Choose one linked-library input mode and keep the path absolute.

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
cmake --build /tmp/tiflash-linked-host-v2 --target gtests_dbms
```

## Run strict-mode proving tests

```bash
TIFORTH_REQUIRE_RUNTIME_EXECUTION=1 \
ctest --test-dir /tmp/tiflash-linked-host-v2 \
  -R TestTiforthExecutionHostV2 \
  --output-on-failure
```
