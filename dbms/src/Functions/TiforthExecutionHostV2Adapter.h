// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Core/Types.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

namespace DB::Tiforth
{

constexpr uint32_t EXECUTION_HOST_V2_ABI_VERSION = 4;

constexpr uint32_t PLAN_KIND_CAST_UTF8_TO_DECIMAL = 1;
constexpr uint32_t PLAN_KIND_INNER_HASH_JOIN_UTF8_KEY_INT64_PAYLOAD = 2;
constexpr uint32_t PLAN_KIND_PROBE_OUTER_HASH_JOIN_INT64_KEY_INT64_PAYLOAD = 6;
constexpr uint32_t PLAN_KIND_BUILD_OUTER_HASH_JOIN_INT64_KEY_INT64_PAYLOAD = 10;

constexpr uint32_t INPUT_ID_SCALAR = 0;
constexpr uint32_t INPUT_ID_BUILD = 0;
constexpr uint32_t INPUT_ID_PROBE = 1;

constexpr uint32_t STATUS_KIND_OK = 0;
constexpr uint32_t STATUS_KIND_PROTOCOL_ERROR = 5;
constexpr uint32_t STATUS_CODE_NONE = 0;
constexpr uint32_t STATUS_CODE_INSTANCE_FINISH_ONLY = 13;
constexpr uint32_t STATUS_CODE_MORE_OUTPUT_AVAILABLE = 29;

constexpr uint32_t PHYSICAL_TYPE_INT64 = 1;
constexpr uint32_t PHYSICAL_TYPE_UTF8 = 2;
constexpr uint32_t PHYSICAL_TYPE_DECIMAL128 = 4;

constexpr uint32_t BATCH_OWNERSHIP_BORROW_WITHIN_CALL = 1;
constexpr uint32_t BATCH_OWNERSHIP_FOREIGN_RETAINABLE = 2;

constexpr uint32_t AMBIENT_REQUIREMENT_SQL_MODE = 1u << 0;
constexpr uint32_t AMBIENT_REQUIREMENT_CHARSET = 1u << 2;
constexpr uint32_t AMBIENT_REQUIREMENT_DEFAULT_COLLATION = 1u << 3;

constexpr uint32_t SQL_MODE_TRUNCATE_AS_WARNING = 2;
constexpr uint32_t SESSION_CHARSET_UTF8MB4 = 1;
constexpr uint32_t DEFAULT_COLLATION_UTF8MB4_BIN = 1;

struct TiforthExecutionBuildRequestV2
{
    uint32_t abi_version;
    uint32_t plan_kind;
    uint32_t ambient_requirement_mask;
    uint32_t sql_mode;
    uint32_t session_charset;
    uint32_t default_collation;
    bool decimal_precision_is_set;
    uint8_t decimal_precision;
    bool decimal_scale_is_set;
    int8_t decimal_scale;
    uint32_t max_block_size;
};

struct TiforthExecutionColumnViewV2
{
    uint32_t physical_type;
    const uint8_t * null_bitmap;
    uint32_t null_bitmap_bit_offset;
    uint32_t row_offset;
    const int64_t * values;
    const int32_t * offsets;
    const uint8_t * data;
    const void * decimal128_words;
    bool decimal_precision_is_set;
    uint8_t decimal_precision;
    bool decimal_scale_is_set;
    int8_t decimal_scale;
};

struct TiforthBatchViewV2
{
    uint32_t abi_version;
    uint32_t ownership_mode;
    uint32_t column_count;
    uint32_t row_count;
    const TiforthExecutionColumnViewV2 * columns;
};

struct TiforthStatusV2
{
    uint32_t abi_version;
    uint32_t kind;
    uint32_t code;
    uint32_t warning_count;
    char message[256];
};

struct TiforthExecutionExecutableHandleV2;
struct TiforthExecutionInstanceHandleV2;

struct TiforthExecutionHostV2Api
{
    using BuildFn = void (*) (
        const TiforthExecutionBuildRequestV2 *,
        TiforthStatusV2 *,
        TiforthExecutionExecutableHandleV2 **);
    using OpenFn = void (*) (
        const TiforthExecutionExecutableHandleV2 *,
        TiforthStatusV2 *,
        TiforthExecutionInstanceHandleV2 **);
    using DriveInputBatchFn = void (*) (
        TiforthExecutionInstanceHandleV2 *,
        uint32_t,
        const TiforthBatchViewV2 *,
        TiforthStatusV2 *,
        TiforthBatchViewV2 *);
    using DriveEndOfInputFn = void (*) (TiforthExecutionInstanceHandleV2 *, uint32_t, TiforthStatusV2 *);
    using DriveEndOfInputWithOutputFn = void (*) (
        TiforthExecutionInstanceHandleV2 *,
        uint32_t,
        TiforthStatusV2 *,
        TiforthBatchViewV2 *);
    using ContinueOutputFn = void (*) (TiforthExecutionInstanceHandleV2 *, TiforthStatusV2 *, TiforthBatchViewV2 *);
    using FinishFn = void (*) (TiforthExecutionInstanceHandleV2 *, TiforthStatusV2 *);
    using ReleaseExecutableFn = void (*) (TiforthExecutionExecutableHandleV2 *);
    using ReleaseInstanceFn = void (*) (TiforthExecutionInstanceHandleV2 *);

    BuildFn build = nullptr;
    OpenFn open = nullptr;
    DriveInputBatchFn drive_input_batch = nullptr;
    DriveEndOfInputFn drive_end_of_input = nullptr;
    DriveEndOfInputWithOutputFn drive_end_of_input_with_output = nullptr;
    ContinueOutputFn continue_output = nullptr;
    FinishFn finish = nullptr;
    ReleaseExecutableFn release_executable = nullptr;
    ReleaseInstanceFn release_instance = nullptr;
};

std::optional<TiforthExecutionHostV2Api> loadExecutionHostV2Api(String & error);

using Utf8Int64Row = std::pair<std::optional<String>, std::optional<int64_t>>;
using Int64Int64Row = std::pair<std::optional<int64_t>, std::optional<int64_t>>;
using JoinOutputRow = std::pair<std::optional<int64_t>, std::optional<int64_t>>;

struct CastRunResult
{
    std::vector<std::optional<String>> output;
    uint32_t warning_count = 0;
};

struct JoinRunResult
{
    std::vector<JoinOutputRow> rows;
    uint32_t warning_count = 0;
};

CastRunResult runCastUtf8ToDecimal(
    const TiforthExecutionHostV2Api & api,
    const std::vector<std::optional<String>> & input,
    size_t partitions,
    uint32_t ownership_mode,
    uint32_t sql_mode,
    uint8_t decimal_precision,
    int8_t decimal_scale,
    uint32_t max_block_size = 0);

JoinRunResult runJoinUtf8KeyInt64Payload(
    const TiforthExecutionHostV2Api & api,
    uint32_t plan_kind,
    const std::vector<Utf8Int64Row> & build_rows,
    const std::vector<Utf8Int64Row> & probe_rows,
    size_t partitions,
    uint32_t ownership_mode,
    uint32_t ambient_requirement_mask,
    uint32_t session_charset,
    uint32_t default_collation,
    uint32_t max_block_size = 0,
    bool build_end_with_output = true,
    bool probe_end_with_output = true);

JoinRunResult runJoinInt64KeyInt64Payload(
    const TiforthExecutionHostV2Api & api,
    uint32_t plan_kind,
    const std::vector<Int64Int64Row> & build_rows,
    const std::vector<Int64Int64Row> & probe_rows,
    size_t partitions,
    uint32_t ownership_mode,
    uint32_t ambient_requirement_mask,
    uint32_t session_charset,
    uint32_t default_collation,
    uint32_t max_block_size = 0,
    bool build_end_with_output = true,
    bool probe_end_with_output = true);

std::vector<JoinOutputRow> canonicalizeJoinRows(std::vector<JoinOutputRow> rows);

} // namespace DB::Tiforth
