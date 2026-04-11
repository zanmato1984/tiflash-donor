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
#include <Functions/TiforthExecutionHostV2Adapter.h>
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

constexpr uint32_t BATCH_OWNERSHIP_BORROW_WITHIN_CALL = 1;
constexpr uint32_t BATCH_OWNERSHIP_FOREIGN_RETAINABLE = 2;
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

#if !defined(TIFORTH_HOST_V2_LINKED_TESTS)
#error "build gtests_tiforth_execution_host_v2 (or gtests_dbms) with -DENABLE_TIFORTH_HOST_V2_LINKED_TESTS=ON and linked libtiforth_ffi_c input"
#endif

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
            "empty_inner_build_input",
            {{"join_key", TiDB::TP::TypeString}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", std::vector<std::optional<String>>{}),
             toNullableVec<Int64>("build_payload", std::vector<std::optional<Int64>>{})});

        context.addMockTable(
            "tiforth_host_v2",
            "empty_inner_probe_input",
            {{"join_key", TiDB::TP::TypeString}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", std::vector<std::optional<String>>{}),
             toNullableVec<Int64>("probe_payload", std::vector<std::optional<Int64>>{})});

        context.addMockTable(
            "tiforth_host_v2",
            "all_null_inner_build_input",
            {{"join_key", TiDB::TP::TypeString}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", {{}, {}, {}}),
             toNullableVec<Int64>("build_payload", {10, 11, 12})});

        context.addMockTable(
            "tiforth_host_v2",
            "all_null_inner_probe_input",
            {{"join_key", TiDB::TP::TypeString}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", {{}, {}, {}}),
             toNullableVec<Int64>("probe_payload", {100, 101, 102})});

        context.addMockTable(
            "tiforth_host_v2",
            "all_null_probe_only_inner_probe_input",
            {{"join_key", TiDB::TP::TypeString}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", {{}, {}, {}}),
             toNullableVec<Int64>("probe_payload", {110, 111, 112})});

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
            "null_fanout_inner_build_input",
            {{"join_key", TiDB::TP::TypeString}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", {"k", "k", "x", "x", {}}),
             toNullableVec<Int64>("build_payload", {10, 11, 20, 21, 30})});

        context.addMockTable(
            "tiforth_host_v2",
            "null_fanout_inner_probe_input",
            {{"join_key", TiDB::TP::TypeString}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", {"k", "k", "x", {}, "y"}),
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
            "build_outer_empty_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", std::vector<std::optional<Int64>>{}),
             toNullableVec<Int64>("probe_payload", std::vector<std::optional<Int64>>{})});

        context.addMockTable(
            "tiforth_host_v2",
            "build_outer_empty_build_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", std::vector<std::optional<Int64>>{}),
             toNullableVec<Int64>("build_payload", std::vector<std::optional<Int64>>{})});

        context.addMockTable(
            "tiforth_host_v2",
            "all_null_build_outer_build_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {{}, {}, {}}),
             toNullableVec<Int64>("build_payload", {10, 11, 12})});

        context.addMockTable(
            "tiforth_host_v2",
            "all_null_build_outer_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {{}, {}}),
             toNullableVec<Int64>("probe_payload", {100, 101})});

        context.addMockTable(
            "tiforth_host_v2",
            "all_null_probe_only_build_outer_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {{}, {}, {}}),
             toNullableVec<Int64>("probe_payload", {110, 111, 112})});

        context.addMockTable(
            "tiforth_host_v2",
            "probe_outer_build_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 1, 5}),
             toNullableVec<Int64>("build_payload", {10, 11, 50})});

        context.addMockTable(
            "tiforth_host_v2",
            "probe_outer_empty_build_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", std::vector<std::optional<Int64>>{}),
             toNullableVec<Int64>("build_payload", std::vector<std::optional<Int64>>{})});

        context.addMockTable(
            "tiforth_host_v2",
            "probe_outer_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 2, {}, 5}),
             toNullableVec<Int64>("probe_payload", {100, 200, 300, 500})});

        context.addMockTable(
            "tiforth_host_v2",
            "probe_outer_empty_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", std::vector<std::optional<Int64>>{}),
             toNullableVec<Int64>("probe_payload", std::vector<std::optional<Int64>>{})});

        context.addMockTable(
            "tiforth_host_v2",
            "all_null_probe_outer_build_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {{}, {}}),
             toNullableVec<Int64>("build_payload", {10, 11})});

        context.addMockTable(
            "tiforth_host_v2",
            "all_null_probe_outer_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {{}, {}, {}}),
             toNullableVec<Int64>("probe_payload", {100, 101, 102})});

        context.addMockTable(
            "tiforth_host_v2",
            "all_null_probe_only_probe_outer_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {{}, {}, {}}),
             toNullableVec<Int64>("probe_payload", {110, 111, 112})});

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

        context.addMockTable(
            "tiforth_host_v2",
            "build_outer_null_fanout_build_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 1, 2, {}, {}}),
             toNullableVec<Int64>("build_payload", {10, 11, 20, 30, 31})});

        context.addMockTable(
            "tiforth_host_v2",
            "build_outer_null_fanout_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 1, 2, {}, 8}),
             toNullableVec<Int64>("probe_payload", {100, 101, 200, 300, 800})});

        context.addMockTable(
            "tiforth_host_v2",
            "probe_outer_null_fanout_build_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 1, 2, {}, {}}),
             toNullableVec<Int64>("build_payload", {10, 11, 20, 30, 31})});

        context.addMockTable(
            "tiforth_host_v2",
            "probe_outer_null_fanout_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 1, 2, {}, 9}),
             toNullableVec<Int64>("probe_payload", {100, 101, 200, 300, 900})});

        context.addMockTable(
            "tiforth_host_v2",
            "disjoint_inner_build_input",
            {{"join_key", TiDB::TP::TypeString}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", {"a", "b", "c", {}}),
             toNullableVec<Int64>("build_payload", {10, 20, 30, 40})});

        context.addMockTable(
            "tiforth_host_v2",
            "disjoint_inner_probe_input",
            {{"join_key", TiDB::TP::TypeString}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", {"x", "y", "z", {}}),
             toNullableVec<Int64>("probe_payload", {100, 200, 300, 400})});

        context.addMockTable(
            "tiforth_host_v2",
            "disjoint_build_outer_build_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 2, 3, {}}),
             toNullableVec<Int64>("build_payload", {10, 20, 30, 40})});

        context.addMockTable(
            "tiforth_host_v2",
            "disjoint_build_outer_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {7, 8, 9, {}}),
             toNullableVec<Int64>("probe_payload", {700, 800, 900, 1000})});

        context.addMockTable(
            "tiforth_host_v2",
            "disjoint_probe_outer_build_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {1, 2, 3, {}}),
             toNullableVec<Int64>("build_payload", {10, 20, 30, 40})});

        context.addMockTable(
            "tiforth_host_v2",
            "disjoint_probe_outer_probe_input",
            {{"join_key", TiDB::TP::TypeLongLong}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("join_key", {7, 8, 9, {}}),
             toNullableVec<Int64>("probe_payload", {700, 800, 900, 1000})});
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

    DonorRunResult runDonorNativeInnerJoinNullFanout(size_t concurrency)
    {
        return runDonorNativeInnerJoinWithInputs(
            concurrency,
            "null_fanout_inner_probe_input",
            "null_fanout_inner_build_input");
    }

    DonorRunResult runDonorNativeInnerJoinDisjoint(size_t concurrency)
    {
        return runDonorNativeInnerJoinWithInputs(
            concurrency,
            "disjoint_inner_probe_input",
            "disjoint_inner_build_input");
    }

    DonorRunResult runDonorNativeInnerJoinEmptyBuild(size_t concurrency)
    {
        return runDonorNativeInnerJoinWithInputs(
            concurrency,
            "probe_input",
            "empty_inner_build_input");
    }

    DonorRunResult runDonorNativeInnerJoinEmptyProbe(size_t concurrency)
    {
        return runDonorNativeInnerJoinWithInputs(
            concurrency,
            "empty_inner_probe_input",
            "build_input");
    }

    DonorRunResult runDonorNativeInnerJoinEmptyBoth(size_t concurrency)
    {
        return runDonorNativeInnerJoinWithInputs(
            concurrency,
            "empty_inner_probe_input",
            "empty_inner_build_input");
    }

    DonorRunResult runDonorNativeInnerJoinAllNull(size_t concurrency)
    {
        return runDonorNativeInnerJoinWithInputs(
            concurrency,
            "all_null_inner_probe_input",
            "all_null_inner_build_input");
    }

    DonorRunResult runDonorNativeInnerJoinAllNullProbeOnly(size_t concurrency)
    {
        return runDonorNativeInnerJoinWithInputs(
            concurrency,
            "all_null_probe_only_inner_probe_input",
            "build_input");
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

    DonorRunResult runDonorNativeBuildOuterJoinNullFanout(size_t concurrency)
    {
        return runDonorNativeBuildOuterJoinWithInputs(
            concurrency,
            "build_outer_null_fanout_probe_input",
            "build_outer_null_fanout_build_input");
    }

    DonorRunResult runDonorNativeBuildOuterJoinDisjoint(size_t concurrency)
    {
        return runDonorNativeBuildOuterJoinWithInputs(
            concurrency,
            "disjoint_build_outer_probe_input",
            "disjoint_build_outer_build_input");
    }

    DonorRunResult runDonorNativeBuildOuterJoinEmptyProbe(size_t concurrency)
    {
        return runDonorNativeBuildOuterJoinWithInputs(
            concurrency,
            "build_outer_empty_probe_input",
            "build_outer_build_input");
    }

    DonorRunResult runDonorNativeBuildOuterJoinEmptyBuild(size_t concurrency)
    {
        return runDonorNativeBuildOuterJoinWithInputs(
            concurrency,
            "build_outer_probe_input",
            "build_outer_empty_build_input");
    }

    DonorRunResult runDonorNativeBuildOuterJoinEmptyBoth(size_t concurrency)
    {
        return runDonorNativeBuildOuterJoinWithInputs(
            concurrency,
            "build_outer_empty_probe_input",
            "build_outer_empty_build_input");
    }

    DonorRunResult runDonorNativeBuildOuterJoinAllNull(size_t concurrency)
    {
        return runDonorNativeBuildOuterJoinWithInputs(
            concurrency,
            "all_null_build_outer_probe_input",
            "all_null_build_outer_build_input");
    }

    DonorRunResult runDonorNativeBuildOuterJoinAllNullProbeOnly(size_t concurrency)
    {
        return runDonorNativeBuildOuterJoinWithInputs(
            concurrency,
            "all_null_probe_only_build_outer_probe_input",
            "build_outer_build_input");
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

    DonorRunResult runDonorNativeProbeOuterJoinNullFanout(size_t concurrency)
    {
        return runDonorNativeProbeOuterJoinWithInputs(
            concurrency,
            "probe_outer_null_fanout_probe_input",
            "probe_outer_null_fanout_build_input");
    }

    DonorRunResult runDonorNativeProbeOuterJoinDisjoint(size_t concurrency)
    {
        return runDonorNativeProbeOuterJoinWithInputs(
            concurrency,
            "disjoint_probe_outer_probe_input",
            "disjoint_probe_outer_build_input");
    }

    DonorRunResult runDonorNativeProbeOuterJoinEmptyBuild(size_t concurrency)
    {
        return runDonorNativeProbeOuterJoinWithInputs(
            concurrency,
            "probe_outer_probe_input",
            "probe_outer_empty_build_input");
    }

    DonorRunResult runDonorNativeProbeOuterJoinEmptyProbe(size_t concurrency)
    {
        return runDonorNativeProbeOuterJoinWithInputs(
            concurrency,
            "probe_outer_empty_probe_input",
            "probe_outer_build_input");
    }

    DonorRunResult runDonorNativeProbeOuterJoinEmptyBoth(size_t concurrency)
    {
        return runDonorNativeProbeOuterJoinWithInputs(
            concurrency,
            "probe_outer_empty_probe_input",
            "probe_outer_empty_build_input");
    }

    DonorRunResult runDonorNativeProbeOuterJoinAllNull(size_t concurrency)
    {
        return runDonorNativeProbeOuterJoinWithInputs(
            concurrency,
            "all_null_probe_outer_probe_input",
            "all_null_probe_outer_build_input");
    }

    DonorRunResult runDonorNativeProbeOuterJoinAllNullProbeOnly(size_t concurrency)
    {
        return runDonorNativeProbeOuterJoinWithInputs(
            concurrency,
            "all_null_probe_only_probe_outer_probe_input",
            "probe_outer_build_input");
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

    static std::vector<JoinInputRow> nullFanoutInnerJoinBuildRows()
    {
        return {
            {String("k"), 10},
            {String("k"), 11},
            {String("x"), 20},
            {String("x"), 21},
            {std::nullopt, 30},
        };
    }

    static std::vector<JoinInputRow> nullFanoutInnerJoinProbeRows()
    {
        return {
            {String("k"), 100},
            {String("k"), 101},
            {String("x"), 200},
            {std::nullopt, 300},
            {String("y"), 400},
        };
    }

    static std::vector<JoinInputRow> disjointInnerJoinBuildRows()
    {
        return {
            {String("a"), 10},
            {String("b"), 20},
            {String("c"), 30},
            {std::nullopt, 40},
        };
    }

    static std::vector<JoinInputRow> disjointInnerJoinProbeRows()
    {
        return {
            {String("x"), 100},
            {String("y"), 200},
            {String("z"), 300},
            {std::nullopt, 400},
        };
    }

    static std::vector<JoinInputRow> emptyInnerJoinBuildRows()
    {
        return {};
    }

    static std::vector<JoinInputRow> emptyInnerJoinProbeRows()
    {
        return {};
    }

    static std::vector<JoinInputRow> allNullInnerJoinBuildRows()
    {
        return {
            {std::nullopt, 10},
            {std::nullopt, 11},
            {std::nullopt, 12},
        };
    }

    static std::vector<JoinInputRow> allNullInnerJoinProbeRows()
    {
        return {
            {std::nullopt, 100},
            {std::nullopt, 101},
            {std::nullopt, 102},
        };
    }

    static std::vector<JoinInputRow> allNullProbeOnlyInnerProbeRows()
    {
        return {
            {std::nullopt, 110},
            {std::nullopt, 111},
            {std::nullopt, 112},
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

    static std::vector<Int64JoinInputRow> emptyBuildOuterBuildRows()
    {
        return {};
    }

    static std::vector<Int64JoinInputRow> defaultBuildOuterProbeRows()
    {
        return {
            {1, 100},
            {5, 500},
        };
    }

    static std::vector<Int64JoinInputRow> emptyBuildOuterProbeRows()
    {
        return {};
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

    static std::vector<Int64JoinInputRow> nullFanoutBuildOuterBuildRows()
    {
        return {
            {1, 10},
            {1, 11},
            {2, 20},
            {std::nullopt, 30},
            {std::nullopt, 31},
        };
    }

    static std::vector<Int64JoinInputRow> nullFanoutBuildOuterProbeRows()
    {
        return {
            {1, 100},
            {1, 101},
            {2, 200},
            {std::nullopt, 300},
            {8, 800},
        };
    }

    static std::vector<Int64JoinInputRow> disjointBuildOuterBuildRows()
    {
        return {
            {1, 10},
            {2, 20},
            {3, 30},
            {std::nullopt, 40},
        };
    }

    static std::vector<Int64JoinInputRow> disjointBuildOuterProbeRows()
    {
        return {
            {7, 700},
            {8, 800},
            {9, 900},
            {std::nullopt, 1000},
        };
    }

    static std::vector<Int64JoinInputRow> allNullBuildOuterBuildRows()
    {
        return {
            {std::nullopt, 10},
            {std::nullopt, 11},
            {std::nullopt, 12},
        };
    }

    static std::vector<Int64JoinInputRow> allNullBuildOuterProbeRows()
    {
        return {
            {std::nullopt, 100},
            {std::nullopt, 101},
        };
    }

    static std::vector<Int64JoinInputRow> allNullProbeOnlyBuildOuterProbeRows()
    {
        return {
            {std::nullopt, 110},
            {std::nullopt, 111},
            {std::nullopt, 112},
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

    static std::vector<Int64JoinInputRow> emptyProbeOuterBuildRows()
    {
        return {};
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

    static std::vector<Int64JoinInputRow> emptyProbeOuterProbeRows()
    {
        return {};
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

    static std::vector<Int64JoinInputRow> nullFanoutProbeOuterBuildRows()
    {
        return {
            {1, 10},
            {1, 11},
            {2, 20},
            {std::nullopt, 30},
            {std::nullopt, 31},
        };
    }

    static std::vector<Int64JoinInputRow> nullFanoutProbeOuterProbeRows()
    {
        return {
            {1, 100},
            {1, 101},
            {2, 200},
            {std::nullopt, 300},
            {9, 900},
        };
    }

    static std::vector<Int64JoinInputRow> disjointProbeOuterBuildRows()
    {
        return {
            {1, 10},
            {2, 20},
            {3, 30},
            {std::nullopt, 40},
        };
    }

    static std::vector<Int64JoinInputRow> disjointProbeOuterProbeRows()
    {
        return {
            {7, 700},
            {8, 800},
            {9, 900},
            {std::nullopt, 1000},
        };
    }

    static std::vector<Int64JoinInputRow> allNullProbeOuterBuildRows()
    {
        return {
            {std::nullopt, 10},
            {std::nullopt, 11},
        };
    }

    static std::vector<Int64JoinInputRow> allNullProbeOuterProbeRows()
    {
        return {
            {std::nullopt, 100},
            {std::nullopt, 101},
            {std::nullopt, 102},
        };
    }

    static std::vector<Int64JoinInputRow> allNullProbeOnlyProbeOuterProbeRows()
    {
        return {
            {std::nullopt, 110},
            {std::nullopt, 111},
            {std::nullopt, 112},
        };
    }

    void runAdapterInnerJoin(
        size_t partitions,
        uint32_t ownership_mode,
        const std::vector<JoinInputRow> & build_rows,
        const std::vector<JoinInputRow> & probe_rows,
        uint32_t max_block_size,
        bool build_end_with_output,
        bool probe_end_with_output,
        AdapterRunResult & result)
    {
        String load_error;
        auto maybe_api = Tiforth::loadExecutionHostV2Api(load_error);
        ASSERT_TRUE(maybe_api.has_value()) << load_error;

        auto run_result = Tiforth::runJoinUtf8KeyInt64Payload(
            maybe_api.value(),
            Tiforth::PLAN_KIND_INNER_HASH_JOIN_UTF8_KEY_INT64_PAYLOAD,
            build_rows,
            probe_rows,
            partitions,
            ownership_mode,
            Tiforth::AMBIENT_REQUIREMENT_CHARSET | Tiforth::AMBIENT_REQUIREMENT_DEFAULT_COLLATION,
            Tiforth::SESSION_CHARSET_UTF8MB4,
            Tiforth::DEFAULT_COLLATION_UTF8MB4_BIN,
            max_block_size,
            build_end_with_output,
            probe_end_with_output);

        result.rows = std::move(run_result.rows);
        result.warning_count = run_result.warning_count;
    }

    void runAdapterInnerJoin(
        size_t partitions,
        uint32_t ownership_mode,
        const std::vector<JoinInputRow> & build_rows,
        const std::vector<JoinInputRow> & probe_rows,
        uint32_t max_block_size,
        AdapterRunResult & result)
    {
        runAdapterInnerJoin(
            partitions,
            ownership_mode,
            build_rows,
            probe_rows,
            max_block_size,
            true,
            true,
            result);
    }

    void runAdapterInnerJoin(size_t partitions, uint32_t ownership_mode, AdapterRunResult & result)
    {
        runAdapterInnerJoin(
            partitions,
            ownership_mode,
            defaultInnerJoinBuildRows(),
            defaultInnerJoinProbeRows(),
            0,
            true,
            true,
            result);
    }

    void runAdapterInt64Join(
        uint32_t plan_kind,
        size_t partitions,
        uint32_t ownership_mode,
        const std::vector<Int64JoinInputRow> & build_rows,
        const std::vector<Int64JoinInputRow> & probe_rows,
        uint32_t max_block_size,
        bool build_end_with_output,
        bool probe_end_with_output,
        AdapterRunResult & result)
    {
        String load_error;
        auto maybe_api = Tiforth::loadExecutionHostV2Api(load_error);
        ASSERT_TRUE(maybe_api.has_value()) << load_error;

        auto run_result = Tiforth::runJoinInt64KeyInt64Payload(
            maybe_api.value(),
            plan_kind,
            build_rows,
            probe_rows,
            partitions,
            ownership_mode,
            0,
            0,
            0,
            max_block_size,
            build_end_with_output,
            probe_end_with_output);

        result.rows = std::move(run_result.rows);
        result.warning_count = run_result.warning_count;
    }

    void runAdapterBuildOuterJoin(
        size_t partitions,
        uint32_t ownership_mode,
        const std::vector<Int64JoinInputRow> & build_rows,
        const std::vector<Int64JoinInputRow> & probe_rows,
        uint32_t max_block_size,
        bool build_end_with_output,
        bool probe_end_with_output,
        AdapterRunResult & result)
    {
        runAdapterInt64Join(
            Tiforth::PLAN_KIND_BUILD_OUTER_HASH_JOIN_INT64_KEY_INT64_PAYLOAD,
            partitions,
            ownership_mode,
            build_rows,
            probe_rows,
            max_block_size,
            build_end_with_output,
            probe_end_with_output,
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
        runAdapterBuildOuterJoin(
            partitions,
            ownership_mode,
            build_rows,
            probe_rows,
            max_block_size,
            true,
            true,
            result);
    }

    void runAdapterBuildOuterJoin(size_t partitions, uint32_t ownership_mode, AdapterRunResult & result)
    {
        runAdapterBuildOuterJoin(
            partitions,
            ownership_mode,
            defaultBuildOuterBuildRows(),
            defaultBuildOuterProbeRows(),
            0,
            true,
            true,
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
            true,
            true,
            result);
    }

    void runAdapterProbeOuterJoin(
        size_t partitions,
        uint32_t ownership_mode,
        const std::vector<Int64JoinInputRow> & build_rows,
        const std::vector<Int64JoinInputRow> & probe_rows,
        uint32_t max_block_size,
        bool build_end_with_output,
        bool probe_end_with_output,
        AdapterRunResult & result)
    {
        runAdapterInt64Join(
            Tiforth::PLAN_KIND_PROBE_OUTER_HASH_JOIN_INT64_KEY_INT64_PAYLOAD,
            partitions,
            ownership_mode,
            build_rows,
            probe_rows,
            max_block_size,
            build_end_with_output,
            probe_end_with_output,
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
        runAdapterProbeOuterJoin(
            partitions,
            ownership_mode,
            build_rows,
            probe_rows,
            max_block_size,
            true,
            true,
            result);
    }

    void runAdapterProbeOuterJoin(size_t partitions, uint32_t ownership_mode, AdapterRunResult & result)
    {
        runAdapterProbeOuterJoin(
            partitions,
            ownership_mode,
            defaultProbeOuterBuildRows(),
            defaultProbeOuterProbeRows(),
            0,
            true,
            true,
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
            true,
            true,
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
    InnerHashJoinPayloadFanoutParityHighPartitionMaxBlockIgnoresRuntimeDylibEnvSerialAndParallel)
{
    ScopedRuntimeDylibEnvOverride runtime_dylib_override(BOGUS_RUNTIME_DYLIB_PATH);
    ASSERT_TRUE(runtime_dylib_override.ok());

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
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    InnerHashJoinPayloadFanoutParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
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
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        fanoutInnerJoinBuildRows(),
        fanoutInnerJoinProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    InnerHashJoinPayloadNullFanoutParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeInnerJoinNullFanout(1);
    auto donor_parallel = runDonorNativeInnerJoinNullFanout(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        nullFanoutInnerJoinBuildRows(),
        nullFanoutInnerJoinProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        nullFanoutInnerJoinBuildRows(),
        nullFanoutInnerJoinProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 6u);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    InnerHashJoinPayloadDisjointParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeInnerJoinDisjoint(1);
    auto donor_parallel = runDonorNativeInnerJoinDisjoint(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        disjointInnerJoinBuildRows(),
        disjointInnerJoinProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        disjointInnerJoinBuildRows(),
        disjointInnerJoinProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 0u);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    InnerHashJoinPayloadEmptyBuildParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeInnerJoinEmptyBuild(1);
    auto donor_parallel = runDonorNativeInnerJoinEmptyBuild(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        emptyInnerJoinBuildRows(),
        defaultInnerJoinProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        emptyInnerJoinBuildRows(),
        defaultInnerJoinProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 0u);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    InnerHashJoinPayloadEmptyProbeParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeInnerJoinEmptyProbe(1);
    auto donor_parallel = runDonorNativeInnerJoinEmptyProbe(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        defaultInnerJoinBuildRows(),
        emptyInnerJoinProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        defaultInnerJoinBuildRows(),
        emptyInnerJoinProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 0u);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    InnerHashJoinPayloadEmptyBothParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeInnerJoinEmptyBoth(1);
    auto donor_parallel = runDonorNativeInnerJoinEmptyBoth(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        emptyInnerJoinBuildRows(),
        emptyInnerJoinProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        emptyInnerJoinBuildRows(),
        emptyInnerJoinProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 0u);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    InnerHashJoinPayloadAllNullKeyParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeInnerJoinAllNull(1);
    auto donor_parallel = runDonorNativeInnerJoinAllNull(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        allNullInnerJoinBuildRows(),
        allNullInnerJoinProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        allNullInnerJoinBuildRows(),
        allNullInnerJoinProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 0u);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    InnerHashJoinPayloadAllNullProbeOnlyParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeInnerJoinAllNullProbeOnly(1);
    auto donor_parallel = runDonorNativeInnerJoinAllNullProbeOnly(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        defaultInnerJoinBuildRows(),
        allNullProbeOnlyInnerProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterInnerJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        defaultInnerJoinBuildRows(),
        allNullProbeOnlyInnerProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 0u);
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
    BuildOuterHashJoinPayloadFanoutParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
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
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        fanoutBuildOuterBuildRows(),
        fanoutBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 9u);

    std::cout << "[tiforth-host-v2-build-outer-join-fanout-legacy-end] serial=8 warnings="
              << adapter_serial.warning_count << " rows=" << adapter_serial.rows.size() << " parallel=8 warnings="
              << adapter_parallel.warning_count << " rows=" << adapter_parallel.rows.size()
              << " donor_warnings=" << donor_serial.warning_count << " donor_rows=" << donor_serial.rows.size()
              << " max_block_size=1 end_with_output=0 parity=ok" << std::endl;
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    BuildOuterHashJoinPayloadDisjointParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeBuildOuterJoinDisjoint(1);
    auto donor_parallel = runDonorNativeBuildOuterJoinDisjoint(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        disjointBuildOuterBuildRows(),
        disjointBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        disjointBuildOuterBuildRows(),
        disjointBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 4u);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    BuildOuterHashJoinPayloadNullFanoutParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeBuildOuterJoinNullFanout(1);
    auto donor_parallel = runDonorNativeBuildOuterJoinNullFanout(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        nullFanoutBuildOuterBuildRows(),
        nullFanoutBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        nullFanoutBuildOuterBuildRows(),
        nullFanoutBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 7u);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    BuildOuterHashJoinPayloadEmptyProbeParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeBuildOuterJoinEmptyProbe(1);
    auto donor_parallel = runDonorNativeBuildOuterJoinEmptyProbe(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        defaultBuildOuterBuildRows(),
        emptyBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        defaultBuildOuterBuildRows(),
        emptyBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), defaultBuildOuterBuildRows().size());
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    BuildOuterHashJoinPayloadEmptyBuildParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeBuildOuterJoinEmptyBuild(1);
    auto donor_parallel = runDonorNativeBuildOuterJoinEmptyBuild(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        emptyBuildOuterBuildRows(),
        defaultBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        emptyBuildOuterBuildRows(),
        defaultBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 0u);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    BuildOuterHashJoinPayloadEmptyBothParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeBuildOuterJoinEmptyBoth(1);
    auto donor_parallel = runDonorNativeBuildOuterJoinEmptyBoth(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        emptyBuildOuterBuildRows(),
        emptyBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        emptyBuildOuterBuildRows(),
        emptyBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 0u);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    BuildOuterHashJoinPayloadAllNullKeyParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeBuildOuterJoinAllNull(1);
    auto donor_parallel = runDonorNativeBuildOuterJoinAllNull(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        allNullBuildOuterBuildRows(),
        allNullBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        allNullBuildOuterBuildRows(),
        allNullBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), allNullBuildOuterBuildRows().size());
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    BuildOuterHashJoinPayloadAllNullProbeOnlyParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeBuildOuterJoinAllNullProbeOnly(1);
    auto donor_parallel = runDonorNativeBuildOuterJoinAllNullProbeOnly(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        defaultBuildOuterBuildRows(),
        allNullProbeOnlyBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterBuildOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        defaultBuildOuterBuildRows(),
        allNullProbeOnlyBuildOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), defaultBuildOuterBuildRows().size());
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
    ProbeOuterHashJoinPayloadFanoutParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
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
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        fanoutProbeOuterBuildRows(),
        fanoutProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 8u);

    std::cout << "[tiforth-host-v2-probe-outer-join-fanout-legacy-end] serial=8 warnings="
              << adapter_serial.warning_count << " rows=" << adapter_serial.rows.size() << " parallel=8 warnings="
              << adapter_parallel.warning_count << " rows=" << adapter_parallel.rows.size()
              << " donor_warnings=" << donor_serial.warning_count << " donor_rows=" << donor_serial.rows.size()
              << " max_block_size=1 end_with_output=0 parity=ok" << std::endl;
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    ProbeOuterHashJoinPayloadDisjointParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeProbeOuterJoinDisjoint(1);
    auto donor_parallel = runDonorNativeProbeOuterJoinDisjoint(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        disjointProbeOuterBuildRows(),
        disjointProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        disjointProbeOuterBuildRows(),
        disjointProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 4u);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    ProbeOuterHashJoinPayloadNullFanoutParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeProbeOuterJoinNullFanout(1);
    auto donor_parallel = runDonorNativeProbeOuterJoinNullFanout(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        nullFanoutProbeOuterBuildRows(),
        nullFanoutProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        nullFanoutProbeOuterBuildRows(),
        nullFanoutProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 7u);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    ProbeOuterHashJoinPayloadEmptyBuildParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeProbeOuterJoinEmptyBuild(1);
    auto donor_parallel = runDonorNativeProbeOuterJoinEmptyBuild(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        emptyProbeOuterBuildRows(),
        defaultProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        emptyProbeOuterBuildRows(),
        defaultProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), defaultProbeOuterProbeRows().size());
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    ProbeOuterHashJoinPayloadEmptyProbeParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeProbeOuterJoinEmptyProbe(1);
    auto donor_parallel = runDonorNativeProbeOuterJoinEmptyProbe(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        defaultProbeOuterBuildRows(),
        emptyProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        defaultProbeOuterBuildRows(),
        emptyProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 0u);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    ProbeOuterHashJoinPayloadEmptyBothParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeProbeOuterJoinEmptyBoth(1);
    auto donor_parallel = runDonorNativeProbeOuterJoinEmptyBoth(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        emptyProbeOuterBuildRows(),
        emptyProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        emptyProbeOuterBuildRows(),
        emptyProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), 0u);
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    ProbeOuterHashJoinPayloadAllNullKeyParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeProbeOuterJoinAllNull(1);
    auto donor_parallel = runDonorNativeProbeOuterJoinAllNull(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        allNullProbeOuterBuildRows(),
        allNullProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        allNullProbeOuterBuildRows(),
        allNullProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), allNullProbeOuterProbeRows().size());
}

TEST_F(
    TestTiforthExecutionHostV2InnerHashJoin,
    ProbeOuterHashJoinPayloadAllNullProbeOnlyParityHighPartitionMaxBlockLegacyEndSerialAndParallel)
{
    auto donor_serial = runDonorNativeProbeOuterJoinAllNullProbeOnly(1);
    auto donor_parallel = runDonorNativeProbeOuterJoinAllNullProbeOnly(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        defaultProbeOuterBuildRows(),
        allNullProbeOnlyProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterProbeOuterJoin(
        8,
        BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        defaultProbeOuterBuildRows(),
        allNullProbeOnlyProbeOuterProbeRows(),
        1,
        false,
        false,
        adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
    ASSERT_EQ(adapter_serial.rows.size(), allNullProbeOnlyProbeOuterProbeRows().size());
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
