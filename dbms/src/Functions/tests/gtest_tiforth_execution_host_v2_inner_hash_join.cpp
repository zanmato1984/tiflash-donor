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

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionHelpers.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <cstddef>
#include <cstdint>
#include <fmt/core.h>
#include <iostream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace DB::tests
{
namespace
{

constexpr uint32_t EXECUTION_HOST_V2_ABI_VERSION = 4;
constexpr uint32_t PLAN_KIND_INNER_HASH_JOIN_UTF8_KEY_INT64_PAYLOAD = 2;
constexpr uint32_t PLAN_KIND_PROBE_OUTER_HASH_JOIN_INT64_KEY_INT64_PAYLOAD = 6;
constexpr uint32_t PLAN_KIND_BUILD_OUTER_HASH_JOIN_INT64_KEY_INT64_PAYLOAD = 10;
constexpr uint32_t INPUT_ID_BUILD = 0;
constexpr uint32_t INPUT_ID_PROBE = 1;
constexpr uint32_t STATUS_KIND_OK = 0;
constexpr uint32_t STATUS_CODE_NONE = 0;
constexpr uint32_t STATUS_CODE_MORE_OUTPUT_AVAILABLE = 29;
constexpr uint32_t PHYSICAL_TYPE_INT64 = 1;
constexpr uint32_t PHYSICAL_TYPE_UTF8 = 2;
constexpr uint32_t BATCH_OWNERSHIP_BORROW_WITHIN_CALL = 1;
constexpr uint32_t BATCH_OWNERSHIP_FOREIGN_RETAINABLE = 2;
constexpr uint32_t AMBIENT_REQUIREMENT_CHARSET = 1u << 2;
constexpr uint32_t AMBIENT_REQUIREMENT_DEFAULT_COLLATION = 1u << 3;
constexpr uint32_t SESSION_CHARSET_UTF8MB4 = 1;
constexpr uint32_t DEFAULT_COLLATION_UTF8MB4_BIN = 1;
constexpr const char * BOGUS_RUNTIME_DYLIB_PATH = "/tmp/tiforth_host_v2_linked_tests_should_not_use_runtime_dispatch.dylib";

class ScopedRuntimeDylibEnvOverride
{
public:
    explicit ScopedRuntimeDylibEnvOverride(const char * value)
    {
        if (const char * current = std::getenv("TIFORTH_FFI_C_DYLIB"); current != nullptr)
        {
            had_previous_value = true;
            previous_value = current;
        }
        applied = (::setenv("TIFORTH_FFI_C_DYLIB", value, 1) == 0);
    }

    ~ScopedRuntimeDylibEnvOverride()
    {
        if (had_previous_value)
            (void)::setenv("TIFORTH_FFI_C_DYLIB", previous_value.c_str(), 1);
        else
            (void)::unsetenv("TIFORTH_FFI_C_DYLIB");
    }

    bool ok() const { return applied; }

private:
    bool had_previous_value = false;
    bool applied = false;
    String previous_value;
};

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

extern "C"
{
void tiforth_execution_host_v2_build(
    const TiforthExecutionBuildRequestV2 * request,
    TiforthStatusV2 * out_status,
    TiforthExecutionExecutableHandleV2 ** out_executable);
void tiforth_execution_host_v2_open(
    const TiforthExecutionExecutableHandleV2 * executable,
    TiforthStatusV2 * out_status,
    TiforthExecutionInstanceHandleV2 ** out_instance);
void tiforth_execution_host_v2_drive_input_batch(
    TiforthExecutionInstanceHandleV2 * instance,
    uint32_t input_id,
    const TiforthBatchViewV2 * input,
    TiforthStatusV2 * out_status,
    TiforthBatchViewV2 * out_output);
void tiforth_execution_host_v2_drive_end_of_input(
    TiforthExecutionInstanceHandleV2 * instance,
    uint32_t input_id,
    TiforthStatusV2 * out_status);
void tiforth_execution_host_v2_drive_end_of_input_with_output(
    TiforthExecutionInstanceHandleV2 * instance,
    uint32_t input_id,
    TiforthStatusV2 * out_status,
    TiforthBatchViewV2 * out_output);
void tiforth_execution_host_v2_continue_output(
    TiforthExecutionInstanceHandleV2 * instance,
    TiforthStatusV2 * out_status,
    TiforthBatchViewV2 * out_output);
void tiforth_execution_host_v2_finish(
    TiforthExecutionInstanceHandleV2 * instance,
    TiforthStatusV2 * out_status);
void tiforth_execution_host_v2_release_executable(TiforthExecutionExecutableHandleV2 * executable);
void tiforth_execution_host_v2_release_instance(TiforthExecutionInstanceHandleV2 * instance);
}

#if !defined(TIFORTH_HOST_V2_LINKED_TESTS)
#error "build gtests_tiforth_execution_host_v2 (or gtests_dbms) with -DENABLE_TIFORTH_HOST_V2_LINKED_TESTS=ON and linked libtiforth_ffi_c input"
#endif

bool isValidRow(const TiforthExecutionColumnViewV2 & column, uint32_t row_count, size_t row)
{
    if (column.null_bitmap == nullptr || row_count == 0)
        return true;
    const size_t bit_index = static_cast<size_t>(column.null_bitmap_bit_offset) + row;
    return (column.null_bitmap[bit_index / 8] & (1u << (bit_index % 8))) != 0;
}

using JoinInputRow = std::pair<std::optional<String>, std::optional<int64_t>>;
using Int64JoinInputRow = std::pair<std::optional<int64_t>, std::optional<int64_t>>;
using JoinOutputRow = std::pair<std::optional<int64_t>, std::optional<int64_t>>;

std::vector<JoinOutputRow> canonicalizeRows(std::vector<JoinOutputRow> rows)
{
    const auto rank_value = [](const std::optional<int64_t> & value) {
        return std::make_pair(value.has_value() ? 1 : 0, value.value_or(0));
    };

    std::sort(
        rows.begin(),
        rows.end(),
        [&](const JoinOutputRow & lhs, const JoinOutputRow & rhs) {
            const auto lhs_build = rank_value(lhs.first);
            const auto rhs_build = rank_value(rhs.first);
            if (lhs_build != rhs_build)
                return lhs_build < rhs_build;
            return rank_value(lhs.second) < rank_value(rhs.second);
        });
    return rows;
}

size_t partitionedChunkCount(size_t row_count, size_t partitions)
{
    if (row_count == 0)
        return 0;
    const size_t chunk_size = std::max<size_t>(1, (row_count + partitions - 1) / partitions);
    return (row_count + chunk_size - 1) / chunk_size;
}

struct JoinBatchOwned
{
    std::vector<uint8_t> key_null_bitmap;
    std::vector<int32_t> key_offsets;
    String key_data;

    std::vector<int64_t> payload_values;
    std::vector<uint8_t> payload_null_bitmap;

    TiforthExecutionColumnViewV2 columns[2]{};
    TiforthBatchViewV2 batch{};

    JoinBatchOwned(const std::vector<JoinInputRow> & rows, uint32_t ownership_mode)
    {
        key_null_bitmap.assign(rows.empty() ? 0 : (rows.size() + 7) / 8, 0);
        key_offsets.reserve(rows.size() + 1);
        key_offsets.push_back(0);

        payload_values.reserve(rows.size());
        payload_null_bitmap.assign(rows.empty() ? 0 : (rows.size() + 7) / 8, 0);

        for (size_t i = 0; i < rows.size(); ++i)
        {
            if (rows[i].first.has_value())
            {
                key_null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
                key_data.append(rows[i].first.value());
            }
            key_offsets.push_back(static_cast<int32_t>(key_data.size()));

            if (rows[i].second.has_value())
            {
                payload_null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
                payload_values.push_back(rows[i].second.value());
            }
            else
            {
                payload_values.push_back(0);
            }
        }

        columns[0].physical_type = PHYSICAL_TYPE_UTF8;
        columns[0].null_bitmap = key_null_bitmap.empty() ? nullptr : key_null_bitmap.data();
        columns[0].null_bitmap_bit_offset = 0;
        columns[0].row_offset = 0;
        columns[0].values = nullptr;
        columns[0].offsets = key_offsets.data();
        columns[0].data = key_data.empty() ? nullptr : reinterpret_cast<const uint8_t *>(key_data.data());
        columns[0].decimal128_words = nullptr;
        columns[0].decimal_precision_is_set = false;
        columns[0].decimal_precision = 0;
        columns[0].decimal_scale_is_set = false;
        columns[0].decimal_scale = 0;

        columns[1].physical_type = PHYSICAL_TYPE_INT64;
        columns[1].null_bitmap = payload_null_bitmap.empty() ? nullptr : payload_null_bitmap.data();
        columns[1].null_bitmap_bit_offset = 0;
        columns[1].row_offset = 0;
        columns[1].values = payload_values.empty() ? nullptr : payload_values.data();
        columns[1].offsets = nullptr;
        columns[1].data = nullptr;
        columns[1].decimal128_words = nullptr;
        columns[1].decimal_precision_is_set = false;
        columns[1].decimal_precision = 0;
        columns[1].decimal_scale_is_set = false;
        columns[1].decimal_scale = 0;

        batch.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        batch.ownership_mode = ownership_mode;
        batch.column_count = 2;
        batch.row_count = static_cast<uint32_t>(rows.size());
        batch.columns = columns;
    }
};

struct Int64JoinBatchOwned
{
    std::vector<int64_t> key_values;
    std::vector<uint8_t> key_null_bitmap;

    std::vector<int64_t> payload_values;
    std::vector<uint8_t> payload_null_bitmap;

    TiforthExecutionColumnViewV2 columns[2]{};
    TiforthBatchViewV2 batch{};

    Int64JoinBatchOwned(const std::vector<Int64JoinInputRow> & rows, uint32_t ownership_mode)
    {
        key_values.reserve(rows.size());
        key_null_bitmap.assign(rows.empty() ? 0 : (rows.size() + 7) / 8, 0);

        payload_values.reserve(rows.size());
        payload_null_bitmap.assign(rows.empty() ? 0 : (rows.size() + 7) / 8, 0);

        for (size_t i = 0; i < rows.size(); ++i)
        {
            if (rows[i].first.has_value())
            {
                key_null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
                key_values.push_back(rows[i].first.value());
            }
            else
            {
                key_values.push_back(0);
            }

            if (rows[i].second.has_value())
            {
                payload_null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
                payload_values.push_back(rows[i].second.value());
            }
            else
            {
                payload_values.push_back(0);
            }
        }

        columns[0].physical_type = PHYSICAL_TYPE_INT64;
        columns[0].null_bitmap = key_null_bitmap.empty() ? nullptr : key_null_bitmap.data();
        columns[0].null_bitmap_bit_offset = 0;
        columns[0].row_offset = 0;
        columns[0].values = key_values.empty() ? nullptr : key_values.data();
        columns[0].offsets = nullptr;
        columns[0].data = nullptr;
        columns[0].decimal128_words = nullptr;
        columns[0].decimal_precision_is_set = false;
        columns[0].decimal_precision = 0;
        columns[0].decimal_scale_is_set = false;
        columns[0].decimal_scale = 0;

        columns[1].physical_type = PHYSICAL_TYPE_INT64;
        columns[1].null_bitmap = payload_null_bitmap.empty() ? nullptr : payload_null_bitmap.data();
        columns[1].null_bitmap_bit_offset = 0;
        columns[1].row_offset = 0;
        columns[1].values = payload_values.empty() ? nullptr : payload_values.data();
        columns[1].offsets = nullptr;
        columns[1].data = nullptr;
        columns[1].decimal128_words = nullptr;
        columns[1].decimal_precision_is_set = false;
        columns[1].decimal_precision = 0;
        columns[1].decimal_scale_is_set = false;
        columns[1].decimal_scale = 0;

        batch.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        batch.ownership_mode = ownership_mode;
        batch.column_count = 2;
        batch.row_count = static_cast<uint32_t>(rows.size());
        batch.columns = columns;
    }
};

void appendJoinOutputRows(const TiforthBatchViewV2 & output, std::vector<JoinOutputRow> & rows)
{
    if (output.row_count == 0)
        return;

    ASSERT_EQ(output.column_count, 2u);

    const auto & build_payload = output.columns[0];
    const auto & probe_payload = output.columns[1];

    ASSERT_EQ(build_payload.physical_type, PHYSICAL_TYPE_INT64);
    ASSERT_EQ(probe_payload.physical_type, PHYSICAL_TYPE_INT64);

    ASSERT_NE(build_payload.values, nullptr);
    ASSERT_NE(probe_payload.values, nullptr);

    const auto * build_values = build_payload.values + build_payload.row_offset;
    const auto * probe_values = probe_payload.values + probe_payload.row_offset;

    for (size_t row = 0; row < output.row_count; ++row)
    {
        const auto build_value = isValidRow(build_payload, output.row_count, row)
            ? std::optional<int64_t>(build_values[row])
            : std::nullopt;
        const auto probe_value = isValidRow(probe_payload, output.row_count, row)
            ? std::optional<int64_t>(probe_values[row])
            : std::nullopt;
        rows.emplace_back(build_value, probe_value);
    }
}

std::vector<std::optional<int64_t>> readNullableInt64Column(const ColumnWithTypeAndName & column)
{
    const auto * nullable = checkAndGetColumn<ColumnNullable>(column.column.get());
    if (nullable == nullptr)
    {
        ADD_FAILURE() << "expected nullable int64 output column";
        return {};
    }

    const auto * nested = checkAndGetColumn<ColumnVector<Int64>>(nullable->getNestedColumnPtr().get());
    if (nested == nullptr)
    {
        ADD_FAILURE() << "expected int64 nested column";
        return {};
    }

    std::vector<std::optional<int64_t>> values;
    values.reserve(nullable->size());

    const auto & nested_data = nested->getData();
    for (size_t row = 0; row < nullable->size(); ++row)
    {
        if (nullable->isNullAt(row))
            values.push_back(std::nullopt);
        else
            values.push_back(nested_data[row]);
    }
    return values;
}

std::vector<JoinOutputRow> joinRowsFromColumns(const ColumnsWithTypeAndName & columns)
{
    EXPECT_EQ(columns.size(), 2u);
    if (columns.size() != 2)
        return {};

    auto build_payload = readNullableInt64Column(columns[0]);
    auto probe_payload = readNullableInt64Column(columns[1]);

    EXPECT_EQ(build_payload.size(), probe_payload.size());
    if (build_payload.size() != probe_payload.size())
        return {};

    std::vector<JoinOutputRow> rows;
    rows.reserve(build_payload.size());
    for (size_t row = 0; row < build_payload.size(); ++row)
        rows.emplace_back(build_payload[row], probe_payload[row]);

    return rows;
}

struct DonorRunResult
{
    std::vector<JoinOutputRow> rows;
    uint32_t warning_count = 0;
};

struct AdapterRunResult
{
    std::vector<JoinOutputRow> rows;
    uint32_t warning_count = 0;
};

class TestTiforthExecutionHostV2InnerHashJoin : public ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable(
            "tiforth_host_v2",
            "build_input",
            {{"join_key", TiDB::TP::TypeString}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", {"k", "k", "x", {}}),
             toNullableVec<Int64>("build_payload", {10, 20, 30, 40})});

        context.addMockTable(
            "tiforth_host_v2",
            "probe_input",
            {{"join_key", TiDB::TP::TypeString}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", {"k", "x", "z", {}}),
             toNullableVec<Int64>("probe_payload", {100, 200, 300, 400})});

        context.addMockTable(
            "tiforth_host_v2",
            "fanout_build_input",
            {{"join_key", TiDB::TP::TypeString}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", {"k", "k", "k", "x", "z", {}}),
             toNullableVec<Int64>("build_payload", {10, 11, 12, 20, 30, 40})});

        context.addMockTable(
            "tiforth_host_v2",
            "fanout_probe_input",
            {{"join_key", TiDB::TP::TypeString}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", {"k", "k", "x", "y", {}}),
             toNullableVec<Int64>("probe_payload", {100, 101, 200, 300, 400})});

        context.addMockTable(
            "tiforth_host_v2",
            "build_outer_build_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 1, 5, 7}),
             toNullableVec<Int64>("build_payload", {10, 11, 50, 70})});

        context.addMockTable(
            "tiforth_host_v2",
            "build_outer_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 5}),
             toNullableVec<Int64>("probe_payload", {100, 500})});

        context.addMockTable(
            "tiforth_host_v2",
            "probe_outer_build_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 1, 5}),
             toNullableVec<Int64>("build_payload", {10, 11, 50})});

        context.addMockTable(
            "tiforth_host_v2",
            "probe_outer_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 2, {}, 5}),
             toNullableVec<Int64>("probe_payload", {100, 200, 300, 500})});

        context.addMockTable(
            "tiforth_host_v2",
            "build_outer_fanout_build_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 1, 1, 2, 7, {}}),
             toNullableVec<Int64>("build_payload", {10, 11, 12, 20, 70, 90})});

        context.addMockTable(
            "tiforth_host_v2",
            "build_outer_fanout_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 1, 2, 8, {}}),
             toNullableVec<Int64>("probe_payload", {100, 101, 200, 800, 900})});

        context.addMockTable(
            "tiforth_host_v2",
            "probe_outer_fanout_build_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 1, 2, {}}),
             toNullableVec<Int64>("build_payload", {10, 11, 20, 90})});

        context.addMockTable(
            "tiforth_host_v2",
            "probe_outer_fanout_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 1, 1, 3, {}}),
             toNullableVec<Int64>("probe_payload", {100, 101, 102, 300, 999})});
    }

    DonorRunResult runDonorNativeInnerJoinWithInputs(
        size_t concurrency,
        const char * probe_table,
        const char * build_table)
    {
        getDAGContext().clearWarnings();
        auto request = context.scan("tiforth_host_v2", probe_table)
                           .join(
                               context.scan("tiforth_host_v2", build_table),
                               tipb::JoinType::TypeInnerJoin,
                               {col("join_key")})
                           .project({"build_payload", "probe_payload"})
                           .build(context);

        DonorRunResult result;
        result.rows = canonicalizeRows(joinRowsFromColumns(executeStreams(request, concurrency)));
        result.warning_count = getDAGContext().getWarningCount();
        return result;
    }

    DonorRunResult runDonorNativeInnerJoin(size_t concurrency)
    {
        return runDonorNativeInnerJoinWithInputs(concurrency, "probe_input", "build_input");
    }

    DonorRunResult runDonorNativeInnerJoinFanout(size_t concurrency)
    {
        return runDonorNativeInnerJoinWithInputs(concurrency, "fanout_probe_input", "fanout_build_input");
    }

    DonorRunResult runDonorNativeBuildOuterJoinWithInputs(
        size_t concurrency,
        const char * probe_table,
        const char * build_table)
    {
        getDAGContext().clearWarnings();
        auto request = context.scan("tiforth_host_v2", probe_table)
                           .join(
                               context.scan("tiforth_host_v2", build_table),
                               tipb::JoinType::TypeRightOuterJoin,
                               {col("join_key")})
                           .project({"build_payload", "probe_payload"})
                           .build(context);

        DonorRunResult result;
        result.rows = canonicalizeRows(joinRowsFromColumns(executeStreams(request, concurrency)));
        result.warning_count = getDAGContext().getWarningCount();
        return result;
    }

    DonorRunResult runDonorNativeBuildOuterJoin(size_t concurrency)
    {
        return runDonorNativeBuildOuterJoinWithInputs(
            concurrency,
            "build_outer_probe_input",
            "build_outer_build_input");
    }

    DonorRunResult runDonorNativeBuildOuterJoinFanout(size_t concurrency)
    {
        return runDonorNativeBuildOuterJoinWithInputs(
            concurrency,
            "build_outer_fanout_probe_input",
            "build_outer_fanout_build_input");
    }

    DonorRunResult runDonorNativeProbeOuterJoinWithInputs(
        size_t concurrency,
        const char * probe_table,
        const char * build_table)
    {
        getDAGContext().clearWarnings();
        auto request = context.scan("tiforth_host_v2", probe_table)
                           .join(
                               context.scan("tiforth_host_v2", build_table),
                               tipb::JoinType::TypeLeftOuterJoin,
                               {col("join_key")})
                           .project({"build_payload", "probe_payload"})
                           .build(context);

        DonorRunResult result;
        result.rows = canonicalizeRows(joinRowsFromColumns(executeStreams(request, concurrency)));
        result.warning_count = getDAGContext().getWarningCount();
        return result;
    }

    DonorRunResult runDonorNativeProbeOuterJoin(size_t concurrency)
    {
        return runDonorNativeProbeOuterJoinWithInputs(
            concurrency,
            "probe_outer_probe_input",
            "probe_outer_build_input");
    }

    DonorRunResult runDonorNativeProbeOuterJoinFanout(size_t concurrency)
    {
        return runDonorNativeProbeOuterJoinWithInputs(
            concurrency,
            "probe_outer_fanout_probe_input",
            "probe_outer_fanout_build_input");
    }

    static std::vector<JoinInputRow> defaultInnerJoinBuildRows()
    {
        return {
            {String("k"), 10},
            {String("k"), 20},
            {String("x"), 30},
            {std::nullopt, 40},
        };
    }

    static std::vector<JoinInputRow> defaultInnerJoinProbeRows()
    {
        return {
            {String("k"), 100},
            {String("x"), 200},
            {String("z"), 300},
            {std::nullopt, 400},
        };
    }

    static std::vector<JoinInputRow> fanoutInnerJoinBuildRows()
    {
        return {
            {String("k"), 10},
            {String("k"), 11},
            {String("k"), 12},
            {String("x"), 20},
            {String("z"), 30},
            {std::nullopt, 40},
        };
    }

    static std::vector<JoinInputRow> fanoutInnerJoinProbeRows()
    {
        return {
            {String("k"), 100},
            {String("k"), 101},
            {String("x"), 200},
            {String("y"), 300},
            {std::nullopt, 400},
        };
    }

    static std::vector<Int64JoinInputRow> defaultBuildOuterBuildRows()
    {
        return {
            {1, 10},
            {1, 11},
            {5, 50},
            {7, 70},
        };
    }

    static std::vector<Int64JoinInputRow> defaultBuildOuterProbeRows()
    {
        return {
            {1, 100},
            {5, 500},
        };
    }

    static std::vector<Int64JoinInputRow> fanoutBuildOuterBuildRows()
    {
        return {
            {1, 10},
            {1, 11},
            {1, 12},
            {2, 20},
            {7, 70},
            {std::nullopt, 90},
        };
    }

    static std::vector<Int64JoinInputRow> fanoutBuildOuterProbeRows()
    {
        return {
            {1, 100},
            {1, 101},
            {2, 200},
            {8, 800},
            {std::nullopt, 900},
        };
    }

    static std::vector<Int64JoinInputRow> defaultProbeOuterBuildRows()
    {
        return {
            {1, 10},
            {1, 11},
            {5, 50},
        };
    }

    static std::vector<Int64JoinInputRow> defaultProbeOuterProbeRows()
    {
        return {
            {1, 100},
            {2, 200},
            {std::nullopt, 300},
            {5, 500},
        };
    }

    static std::vector<Int64JoinInputRow> fanoutProbeOuterBuildRows()
    {
        return {
            {1, 10},
            {1, 11},
            {2, 20},
            {std::nullopt, 90},
        };
    }

    static std::vector<Int64JoinInputRow> fanoutProbeOuterProbeRows()
    {
        return {
            {1, 100},
            {1, 101},
            {1, 102},
            {3, 300},
            {std::nullopt, 999},
        };
    }

    void runAdapterInnerJoin(
        size_t partitions,
        uint32_t ownership_mode,
        const std::vector<JoinInputRow> & build_rows,
        const std::vector<JoinInputRow> & probe_rows,
        uint32_t max_block_size,
        AdapterRunResult & result)
    {
        TiforthExecutionBuildRequestV2 build_request{};
        build_request.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        build_request.plan_kind = PLAN_KIND_INNER_HASH_JOIN_UTF8_KEY_INT64_PAYLOAD;
        build_request.ambient_requirement_mask =
            AMBIENT_REQUIREMENT_CHARSET | AMBIENT_REQUIREMENT_DEFAULT_COLLATION;
        build_request.sql_mode = 0;
        build_request.session_charset = SESSION_CHARSET_UTF8MB4;
        build_request.default_collation = DEFAULT_COLLATION_UTF8MB4_BIN;
        build_request.decimal_precision_is_set = false;
        build_request.decimal_precision = 0;
        build_request.decimal_scale_is_set = false;
        build_request.decimal_scale = 0;
        build_request.max_block_size = max_block_size;

        TiforthStatusV2 status{};
        status.abi_version = EXECUTION_HOST_V2_ABI_VERSION;

        TiforthExecutionExecutableHandleV2 * executable = nullptr;
        tiforth_execution_host_v2_build(&build_request, &status, &executable);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
        ASSERT_NE(executable, nullptr);

        TiforthExecutionInstanceHandleV2 * instance = nullptr;
        tiforth_execution_host_v2_open(executable, &status, &instance);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
        ASSERT_NE(instance, nullptr);

        result.rows.clear();
        result.warning_count = 0;
        std::vector<JoinBatchOwned> retained_batches;
        if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
        {
            retained_batches.reserve(
                partitionedChunkCount(build_rows.size(), partitions)
                + partitionedChunkCount(probe_rows.size(), partitions));
        }

        auto drain_output = [&]() {
            while (status.code == STATUS_CODE_MORE_OUTPUT_AVAILABLE)
            {
                TiforthBatchViewV2 continued_output{};
                continued_output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
                tiforth_execution_host_v2_continue_output(instance, &status, &continued_output);

                ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
                result.warning_count += status.warning_count;
                appendJoinOutputRows(continued_output, result.rows);
            }
            ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
        };

        auto drive_input_rows = [&](const std::vector<JoinInputRow> & rows, uint32_t input_id) {
            const size_t chunk_size = std::max<size_t>(1, (rows.size() + partitions - 1) / partitions);
            for (size_t start = 0; start < rows.size(); start += chunk_size)
            {
                const size_t end = std::min(rows.size(), start + chunk_size);
                std::vector<JoinInputRow> chunk(
                    rows.begin() + static_cast<ptrdiff_t>(start),
                    rows.begin() + static_cast<ptrdiff_t>(end));

                const TiforthBatchViewV2 * input_batch = nullptr;
                std::optional<JoinBatchOwned> borrowed_batch;
                if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
                {
                    retained_batches.emplace_back(chunk, ownership_mode);
                    input_batch = &retained_batches.back().batch;
                }
                else
                {
                    borrowed_batch.emplace(chunk, ownership_mode);
                    input_batch = &borrowed_batch->batch;
                }

                TiforthBatchViewV2 output{};
                output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
                tiforth_execution_host_v2_drive_input_batch(instance, input_id, input_batch, &status, &output);

                ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
                result.warning_count += status.warning_count;
                appendJoinOutputRows(output, result.rows);
                drain_output();
            }
        };

        drive_input_rows(build_rows, INPUT_ID_BUILD);

        TiforthBatchViewV2 build_end_output{};
        build_end_output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        tiforth_execution_host_v2_drive_end_of_input_with_output(instance, INPUT_ID_BUILD, &status, &build_end_output);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        result.warning_count += status.warning_count;
        appendJoinOutputRows(build_end_output, result.rows);
        drain_output();

        drive_input_rows(probe_rows, INPUT_ID_PROBE);

        TiforthBatchViewV2 probe_end_output{};
        probe_end_output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        tiforth_execution_host_v2_drive_end_of_input_with_output(instance, INPUT_ID_PROBE, &status, &probe_end_output);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        result.warning_count += status.warning_count;
        appendJoinOutputRows(probe_end_output, result.rows);
        drain_output();

        tiforth_execution_host_v2_finish(instance, &status);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;

        tiforth_execution_host_v2_release_instance(instance);
        tiforth_execution_host_v2_release_executable(executable);

        result.rows = canonicalizeRows(std::move(result.rows));
    }

    void runAdapterInnerJoin(size_t partitions, uint32_t ownership_mode, AdapterRunResult & result)
    {
        runAdapterInnerJoin(
            partitions,
            ownership_mode,
            defaultInnerJoinBuildRows(),
            defaultInnerJoinProbeRows(),
            0,
            result);
    }

    void runAdapterBuildOuterJoin(
        size_t partitions,
        uint32_t ownership_mode,
        const std::vector<Int64JoinInputRow> & build_rows,
        const std::vector<Int64JoinInputRow> & probe_rows,
        uint32_t max_block_size,
        AdapterRunResult & result)
    {
        TiforthExecutionBuildRequestV2 build_request{};
        build_request.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        build_request.plan_kind = PLAN_KIND_BUILD_OUTER_HASH_JOIN_INT64_KEY_INT64_PAYLOAD;
        build_request.ambient_requirement_mask = 0;
        build_request.sql_mode = 0;
        build_request.session_charset = 0;
        build_request.default_collation = 0;
        build_request.decimal_precision_is_set = false;
        build_request.decimal_precision = 0;
        build_request.decimal_scale_is_set = false;
        build_request.decimal_scale = 0;
        build_request.max_block_size = max_block_size;

        TiforthStatusV2 status{};
        status.abi_version = EXECUTION_HOST_V2_ABI_VERSION;

        TiforthExecutionExecutableHandleV2 * executable = nullptr;
        tiforth_execution_host_v2_build(&build_request, &status, &executable);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
        ASSERT_NE(executable, nullptr);

        TiforthExecutionInstanceHandleV2 * instance = nullptr;
        tiforth_execution_host_v2_open(executable, &status, &instance);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
        ASSERT_NE(instance, nullptr);

        result.rows.clear();
        result.warning_count = 0;

        std::vector<Int64JoinBatchOwned> retained_batches;
        if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
        {
            retained_batches.reserve(
                partitionedChunkCount(build_rows.size(), partitions)
                + partitionedChunkCount(probe_rows.size(), partitions));
        }

        auto drain_output = [&]() {
            while (status.code == STATUS_CODE_MORE_OUTPUT_AVAILABLE)
            {
                TiforthBatchViewV2 continued_output{};
                continued_output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
                tiforth_execution_host_v2_continue_output(instance, &status, &continued_output);

                ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
                result.warning_count += status.warning_count;
                appendJoinOutputRows(continued_output, result.rows);
            }
            ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
        };

        auto drive_input_rows = [&](const std::vector<Int64JoinInputRow> & rows, uint32_t input_id) {
            const size_t chunk_size = std::max<size_t>(1, (rows.size() + partitions - 1) / partitions);
            for (size_t start = 0; start < rows.size(); start += chunk_size)
            {
                const size_t end = std::min(rows.size(), start + chunk_size);
                std::vector<Int64JoinInputRow> chunk(
                    rows.begin() + static_cast<ptrdiff_t>(start),
                    rows.begin() + static_cast<ptrdiff_t>(end));

                const TiforthBatchViewV2 * input_batch = nullptr;
                std::optional<Int64JoinBatchOwned> borrowed_batch;
                if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
                {
                    retained_batches.emplace_back(chunk, ownership_mode);
                    input_batch = &retained_batches.back().batch;
                }
                else
                {
                    borrowed_batch.emplace(chunk, ownership_mode);
                    input_batch = &borrowed_batch->batch;
                }

                TiforthBatchViewV2 output{};
                output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
                tiforth_execution_host_v2_drive_input_batch(instance, input_id, input_batch, &status, &output);

                ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
                result.warning_count += status.warning_count;
                appendJoinOutputRows(output, result.rows);
                drain_output();
            }
        };

        drive_input_rows(build_rows, INPUT_ID_BUILD);

        TiforthBatchViewV2 build_end_output{};
        build_end_output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        tiforth_execution_host_v2_drive_end_of_input_with_output(instance, INPUT_ID_BUILD, &status, &build_end_output);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        result.warning_count += status.warning_count;
        appendJoinOutputRows(build_end_output, result.rows);
        drain_output();

        drive_input_rows(probe_rows, INPUT_ID_PROBE);

        TiforthBatchViewV2 probe_end_output{};
        probe_end_output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        tiforth_execution_host_v2_drive_end_of_input_with_output(instance, INPUT_ID_PROBE, &status, &probe_end_output);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        result.warning_count += status.warning_count;
        appendJoinOutputRows(probe_end_output, result.rows);
        drain_output();

        tiforth_execution_host_v2_finish(instance, &status);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;

        tiforth_execution_host_v2_release_instance(instance);
        tiforth_execution_host_v2_release_executable(executable);

        result.rows = canonicalizeRows(std::move(result.rows));
    }

    void runAdapterBuildOuterJoin(size_t partitions, uint32_t ownership_mode, AdapterRunResult & result)
    {
        runAdapterBuildOuterJoin(
            partitions,
            ownership_mode,
            defaultBuildOuterBuildRows(),
            defaultBuildOuterProbeRows(),
            0,
            result);
    }

    void runAdapterBuildOuterJoin(
        size_t partitions,
        uint32_t ownership_mode,
        uint32_t max_block_size,
        AdapterRunResult & result)
    {
        runAdapterBuildOuterJoin(
            partitions,
            ownership_mode,
            defaultBuildOuterBuildRows(),
            defaultBuildOuterProbeRows(),
            max_block_size,
            result);
    }

    void runAdapterProbeOuterJoin(
        size_t partitions,
        uint32_t ownership_mode,
        const std::vector<Int64JoinInputRow> & build_rows,
        const std::vector<Int64JoinInputRow> & probe_rows,
        uint32_t max_block_size,
        AdapterRunResult & result)
    {
        TiforthExecutionBuildRequestV2 build_request{};
        build_request.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        build_request.plan_kind = PLAN_KIND_PROBE_OUTER_HASH_JOIN_INT64_KEY_INT64_PAYLOAD;
        build_request.ambient_requirement_mask = 0;
        build_request.sql_mode = 0;
        build_request.session_charset = 0;
        build_request.default_collation = 0;
        build_request.decimal_precision_is_set = false;
        build_request.decimal_precision = 0;
        build_request.decimal_scale_is_set = false;
        build_request.decimal_scale = 0;
        build_request.max_block_size = max_block_size;

        TiforthStatusV2 status{};
        status.abi_version = EXECUTION_HOST_V2_ABI_VERSION;

        TiforthExecutionExecutableHandleV2 * executable = nullptr;
        tiforth_execution_host_v2_build(&build_request, &status, &executable);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
        ASSERT_NE(executable, nullptr);

        TiforthExecutionInstanceHandleV2 * instance = nullptr;
        tiforth_execution_host_v2_open(executable, &status, &instance);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
        ASSERT_NE(instance, nullptr);

        result.rows.clear();
        result.warning_count = 0;

        std::vector<Int64JoinBatchOwned> retained_batches;
        if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
        {
            retained_batches.reserve(
                partitionedChunkCount(build_rows.size(), partitions)
                + partitionedChunkCount(probe_rows.size(), partitions));
        }

        auto drain_output = [&]() {
            while (status.code == STATUS_CODE_MORE_OUTPUT_AVAILABLE)
            {
                TiforthBatchViewV2 continued_output{};
                continued_output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
                tiforth_execution_host_v2_continue_output(instance, &status, &continued_output);

                ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
                result.warning_count += status.warning_count;
                appendJoinOutputRows(continued_output, result.rows);
            }
            ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
        };

        auto drive_input_rows = [&](const std::vector<Int64JoinInputRow> & rows, uint32_t input_id) {
            const size_t chunk_size = std::max<size_t>(1, (rows.size() + partitions - 1) / partitions);
            for (size_t start = 0; start < rows.size(); start += chunk_size)
            {
                const size_t end = std::min(rows.size(), start + chunk_size);
                std::vector<Int64JoinInputRow> chunk(
                    rows.begin() + static_cast<ptrdiff_t>(start),
                    rows.begin() + static_cast<ptrdiff_t>(end));

                const TiforthBatchViewV2 * input_batch = nullptr;
                std::optional<Int64JoinBatchOwned> borrowed_batch;
                if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
                {
                    retained_batches.emplace_back(chunk, ownership_mode);
                    input_batch = &retained_batches.back().batch;
                }
                else
                {
                    borrowed_batch.emplace(chunk, ownership_mode);
                    input_batch = &borrowed_batch->batch;
                }

                TiforthBatchViewV2 output{};
                output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
                tiforth_execution_host_v2_drive_input_batch(instance, input_id, input_batch, &status, &output);

                ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
                result.warning_count += status.warning_count;
                appendJoinOutputRows(output, result.rows);
                drain_output();
            }
        };

        drive_input_rows(build_rows, INPUT_ID_BUILD);

        TiforthBatchViewV2 build_end_output{};
        build_end_output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        tiforth_execution_host_v2_drive_end_of_input_with_output(instance, INPUT_ID_BUILD, &status, &build_end_output);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        result.warning_count += status.warning_count;
        appendJoinOutputRows(build_end_output, result.rows);
        drain_output();

        drive_input_rows(probe_rows, INPUT_ID_PROBE);

        TiforthBatchViewV2 probe_end_output{};
        probe_end_output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        tiforth_execution_host_v2_drive_end_of_input_with_output(instance, INPUT_ID_PROBE, &status, &probe_end_output);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        result.warning_count += status.warning_count;
        appendJoinOutputRows(probe_end_output, result.rows);
        drain_output();

        tiforth_execution_host_v2_finish(instance, &status);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;

        tiforth_execution_host_v2_release_instance(instance);
        tiforth_execution_host_v2_release_executable(executable);

        result.rows = canonicalizeRows(std::move(result.rows));
    }

    void runAdapterProbeOuterJoin(size_t partitions, uint32_t ownership_mode, AdapterRunResult & result)
    {
        runAdapterProbeOuterJoin(
            partitions,
            ownership_mode,
            defaultProbeOuterBuildRows(),
            defaultProbeOuterProbeRows(),
            0,
            result);
    }

    void runAdapterProbeOuterJoin(
        size_t partitions,
        uint32_t ownership_mode,
        uint32_t max_block_size,
        AdapterRunResult & result)
    {
        runAdapterProbeOuterJoin(
            partitions,
            ownership_mode,
            defaultProbeOuterBuildRows(),
            defaultProbeOuterProbeRows(),
            max_block_size,
            result);
    }
};

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, InnerHashJoinPayloadParitySerialAndParallel)
{
    auto donor_serial = runDonorNativeInnerJoin(1);
    auto donor_parallel = runDonorNativeInnerJoin(2);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterInnerJoin(1, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterInnerJoin(2, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);

    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);

    std::cout << "[tiforth-host-v2-inner-join] serial=1 warnings=" << adapter_serial.warning_count
              << " rows=" << adapter_serial.rows.size() << " parallel=2 warnings=" << adapter_parallel.warning_count
              << " rows=" << adapter_parallel.rows.size() << " donor_warnings=" << donor_serial.warning_count
              << " donor_rows=" << donor_serial.rows.size() << " parity=ok" << std::endl;
}

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, InnerHashJoinPayloadParityIgnoresRuntimeDylibEnvSerialAndParallel)
{
    ScopedRuntimeDylibEnvOverride runtime_dylib_override(BOGUS_RUNTIME_DYLIB_PATH);
    ASSERT_TRUE(runtime_dylib_override.ok());

    auto donor_serial = runDonorNativeInnerJoin(1);
    auto donor_parallel = runDonorNativeInnerJoin(2);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterInnerJoin(1, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterInnerJoin(2, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
}

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, InnerHashJoinPayloadParityHighPartitionMaxBlockSerialAndParallel)
{
    auto donor_serial = runDonorNativeInnerJoin(1);
    auto donor_parallel = runDonorNativeInnerJoin(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        defaultInnerJoinBuildRows(),
        defaultInnerJoinProbeRows(),
        1,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        defaultInnerJoinBuildRows(),
        defaultInnerJoinProbeRows(),
        1,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);

    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);

    std::cout << "[tiforth-host-v2-inner-join-high-partition] serial=8 warnings=" << adapter_serial.warning_count
              << " rows=" << adapter_serial.rows.size() << " parallel=8 warnings=" << adapter_parallel.warning_count
              << " rows=" << adapter_parallel.rows.size() << " donor_warnings=" << donor_serial.warning_count
              << " donor_rows=" << donor_serial.rows.size() << " max_block_size=1 parity=ok" << std::endl;
}

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, InnerHashJoinPayloadFanoutParityHighPartitionMaxBlockSerialAndParallel)
{
    auto donor_serial = runDonorNativeInnerJoinFanout(1);
    auto donor_parallel = runDonorNativeInnerJoinFanout(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        fanoutInnerJoinBuildRows(),
        fanoutInnerJoinProbeRows(),
        1,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        fanoutInnerJoinBuildRows(),
        fanoutInnerJoinProbeRows(),
        1,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);

    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 7u);

    std::cout << "[tiforth-host-v2-inner-join-fanout] serial=8 warnings=" << adapter_serial.warning_count
              << " rows=" << adapter_serial.rows.size() << " parallel=8 warnings=" << adapter_parallel.warning_count
              << " rows=" << adapter_parallel.rows.size() << " donor_warnings=" << donor_serial.warning_count
              << " donor_rows=" << donor_serial.rows.size() << " max_block_size=1 parity=ok" << std::endl;
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    InnerHashJoinPayloadParityHighPartitionMaxBlockIgnoresRuntimeDylibEnvSerialAndParallel)
{
    ScopedRuntimeDylibEnvOverride runtime_dylib_override(BOGUS_RUNTIME_DYLIB_PATH);
    ASSERT_TRUE(runtime_dylib_override.ok());

    auto donor_serial = runDonorNativeInnerJoin(1);
    auto donor_parallel = runDonorNativeInnerJoin(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        defaultInnerJoinBuildRows(),
        defaultInnerJoinProbeRows(),
        1,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        defaultInnerJoinBuildRows(),
        defaultInnerJoinProbeRows(),
        1,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
}

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, BuildOuterHashJoinPayloadParitySerialAndParallel)
{
    auto donor_serial = runDonorNativeBuildOuterJoin(1);
    auto donor_parallel = runDonorNativeBuildOuterJoin(2);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterBuildOuterJoin(1, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterBuildOuterJoin(2, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);

    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);

    std::cout << "[tiforth-host-v2-build-outer-join] serial=1 warnings=" << adapter_serial.warning_count
              << " rows=" << adapter_serial.rows.size() << " parallel=2 warnings=" << adapter_parallel.warning_count
              << " rows=" << adapter_parallel.rows.size() << " donor_warnings=" << donor_serial.warning_count
              << " donor_rows=" << donor_serial.rows.size() << " parity=ok" << std::endl;
}

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, BuildOuterHashJoinPayloadParityIgnoresRuntimeDylibEnvSerialAndParallel)
{
    ScopedRuntimeDylibEnvOverride runtime_dylib_override(BOGUS_RUNTIME_DYLIB_PATH);
    ASSERT_TRUE(runtime_dylib_override.ok());

    auto donor_serial = runDonorNativeBuildOuterJoin(1);
    auto donor_parallel = runDonorNativeBuildOuterJoin(2);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterBuildOuterJoin(1, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterBuildOuterJoin(2, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
}

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, BuildOuterHashJoinPayloadParityHighPartitionMaxBlockSerialAndParallel)
{
    auto donor_serial = runDonorNativeBuildOuterJoin(1);
    auto donor_parallel = runDonorNativeBuildOuterJoin(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterBuildOuterJoin(8, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, 1, adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterBuildOuterJoin(8, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, 1, adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);

    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);

    std::cout << "[tiforth-host-v2-build-outer-join-high-partition] serial=8 warnings=" << adapter_serial.warning_count
              << " rows=" << adapter_serial.rows.size() << " parallel=8 warnings=" << adapter_parallel.warning_count
              << " rows=" << adapter_parallel.rows.size() << " donor_warnings=" << donor_serial.warning_count
              << " donor_rows=" << donor_serial.rows.size() << " max_block_size=1 parity=ok" << std::endl;
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    BuildOuterHashJoinPayloadFanoutParityHighPartitionMaxBlockSerialAndParallel)
{
    auto donor_serial = runDonorNativeBuildOuterJoinFanout(1);
    auto donor_parallel = runDonorNativeBuildOuterJoinFanout(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        fanoutBuildOuterBuildRows(),
        fanoutBuildOuterProbeRows(),
        1,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        fanoutBuildOuterBuildRows(),
        fanoutBuildOuterProbeRows(),
        1,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 9u);

    std::cout << "[tiforth-host-v2-build-outer-join-fanout] serial=8 warnings=" << adapter_serial.warning_count
              << " rows=" << adapter_serial.rows.size() << " parallel=8 warnings=" << adapter_parallel.warning_count
              << " rows=" << adapter_parallel.rows.size() << " donor_warnings=" << donor_serial.warning_count
              << " donor_rows=" << donor_serial.rows.size() << " max_block_size=1 parity=ok" << std::endl;
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    BuildOuterHashJoinPayloadParityHighPartitionMaxBlockIgnoresRuntimeDylibEnvSerialAndParallel)
{
    ScopedRuntimeDylibEnvOverride runtime_dylib_override(BOGUS_RUNTIME_DYLIB_PATH);
    ASSERT_TRUE(runtime_dylib_override.ok());

    auto donor_serial = runDonorNativeBuildOuterJoin(1);
    auto donor_parallel = runDonorNativeBuildOuterJoin(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterBuildOuterJoin(8, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, 1, adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterBuildOuterJoin(8, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, 1, adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
}

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, ProbeOuterHashJoinPayloadParitySerialAndParallel)
{
    auto donor_serial = runDonorNativeProbeOuterJoin(1);
    auto donor_parallel = runDonorNativeProbeOuterJoin(2);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterProbeOuterJoin(1, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterProbeOuterJoin(2, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);

    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);

    std::cout << "[tiforth-host-v2-probe-outer-join] serial=1 warnings=" << adapter_serial.warning_count
              << " rows=" << adapter_serial.rows.size() << " parallel=2 warnings=" << adapter_parallel.warning_count
              << " rows=" << adapter_parallel.rows.size() << " donor_warnings=" << donor_serial.warning_count
              << " donor_rows=" << donor_serial.rows.size() << " parity=ok" << std::endl;
}

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, ProbeOuterHashJoinPayloadParityIgnoresRuntimeDylibEnvSerialAndParallel)
{
    ScopedRuntimeDylibEnvOverride runtime_dylib_override(BOGUS_RUNTIME_DYLIB_PATH);
    ASSERT_TRUE(runtime_dylib_override.ok());

    auto donor_serial = runDonorNativeProbeOuterJoin(1);
    auto donor_parallel = runDonorNativeProbeOuterJoin(2);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterProbeOuterJoin(1, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterProbeOuterJoin(2, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
}

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, ProbeOuterHashJoinPayloadParityHighPartitionMaxBlockSerialAndParallel)
{
    auto donor_serial = runDonorNativeProbeOuterJoin(1);
    auto donor_parallel = runDonorNativeProbeOuterJoin(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterProbeOuterJoin(8, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, 1, adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterProbeOuterJoin(8, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, 1, adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);

    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);

    std::cout << "[tiforth-host-v2-probe-outer-join-high-partition] serial=8 warnings=" << adapter_serial.warning_count
              << " rows=" << adapter_serial.rows.size() << " parallel=8 warnings=" << adapter_parallel.warning_count
              << " rows=" << adapter_parallel.rows.size() << " donor_warnings=" << donor_serial.warning_count
              << " donor_rows=" << donor_serial.rows.size() << " max_block_size=1 parity=ok" << std::endl;
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    ProbeOuterHashJoinPayloadFanoutParityHighPartitionMaxBlockSerialAndParallel)
{
    auto donor_serial = runDonorNativeProbeOuterJoinFanout(1);
    auto donor_parallel = runDonorNativeProbeOuterJoinFanout(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        fanoutProbeOuterBuildRows(),
        fanoutProbeOuterProbeRows(),
        1,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        fanoutProbeOuterBuildRows(),
        fanoutProbeOuterProbeRows(),
        1,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 8u);

    std::cout << "[tiforth-host-v2-probe-outer-join-fanout] serial=8 warnings=" << adapter_serial.warning_count
              << " rows=" << adapter_serial.rows.size() << " parallel=8 warnings=" << adapter_parallel.warning_count
              << " rows=" << adapter_parallel.rows.size() << " donor_warnings=" << donor_serial.warning_count
              << " donor_rows=" << donor_serial.rows.size() << " max_block_size=1 parity=ok" << std::endl;
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    ProbeOuterHashJoinPayloadParityHighPartitionMaxBlockIgnoresRuntimeDylibEnvSerialAndParallel)
{
    ScopedRuntimeDylibEnvOverride runtime_dylib_override(BOGUS_RUNTIME_DYLIB_PATH);
    ASSERT_TRUE(runtime_dylib_override.ok());

    auto donor_serial = runDonorNativeProbeOuterJoin(1);
    auto donor_parallel = runDonorNativeProbeOuterJoin(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterProbeOuterJoin(8, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, 1, adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterProbeOuterJoin(8, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, 1, adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
}

} // namespace
} // namespace DB::tests
