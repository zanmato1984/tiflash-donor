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
#include <Functions/TiforthExecutionHostV2Adapter.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <gtest/gtest.h>

#include <iostream>
#include <utility>
#include <vector>

namespace DB::tests
{
namespace
{

using JoinOutputRow = Tiforth::JoinOutputRow;

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
    }

    DonorRunResult runDonorNativeInnerJoin(size_t concurrency)
    {
        getDAGContext().clearWarnings();
        auto request = context.scan("tiforth_host_v2", "probe_input")
                           .join(
                               context.scan("tiforth_host_v2", "build_input"),
                               tipb::JoinType::TypeInnerJoin,
                               {col("join_key")})
                           .project({"build_payload", "probe_payload"})
                           .build(context);

        DonorRunResult result;
        result.rows = Tiforth::canonicalizeJoinRows(joinRowsFromColumns(executeStreams(request, concurrency)));
        result.warning_count = getDAGContext().getWarningCount();
        return result;
    }

    DonorRunResult runDonorNativeBuildOuterJoin(size_t concurrency)
    {
        getDAGContext().clearWarnings();
        auto request = context.scan("tiforth_host_v2", "build_outer_probe_input")
                           .join(
                               context.scan("tiforth_host_v2", "build_outer_build_input"),
                               tipb::JoinType::TypeRightOuterJoin,
                               {col("join_key")})
                           .project({"build_payload", "probe_payload"})
                           .build(context);

        DonorRunResult result;
        result.rows = Tiforth::canonicalizeJoinRows(joinRowsFromColumns(executeStreams(request, concurrency)));
        result.warning_count = getDAGContext().getWarningCount();
        return result;
    }

    DonorRunResult runDonorNativeProbeOuterJoin(size_t concurrency)
    {
        getDAGContext().clearWarnings();
        auto request = context.scan("tiforth_host_v2", "probe_outer_probe_input")
                           .join(
                               context.scan("tiforth_host_v2", "probe_outer_build_input"),
                               tipb::JoinType::TypeLeftOuterJoin,
                               {col("join_key")})
                           .project({"build_payload", "probe_payload"})
                           .build(context);

        DonorRunResult result;
        result.rows = Tiforth::canonicalizeJoinRows(joinRowsFromColumns(executeStreams(request, concurrency)));
        result.warning_count = getDAGContext().getWarningCount();
        return result;
    }
};

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, InnerHashJoinPayloadParitySerialAndParallel)
{
    const bool strict_runtime_execution = Tiforth::requiresStrictRuntimeExecution();
    String load_error;
    auto maybe_api = Tiforth::loadExecutionHostV2Api(load_error);
    if (!maybe_api.has_value())
    {
        if (strict_runtime_execution)
            GTEST_FAIL() << load_error;
        SUCCEED() << load_error;
        return;
    }

    auto api = std::move(maybe_api.value());

    auto donor_serial = runDonorNativeInnerJoin(1);
    auto donor_parallel = runDonorNativeInnerJoin(2);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    const std::vector<Tiforth::Utf8Int64Row> build_rows = {
        {String("k"), 10},
        {String("k"), 20},
        {String("x"), 30},
        {std::nullopt, 40},
    };
    const std::vector<Tiforth::Utf8Int64Row> probe_rows = {
        {String("k"), 100},
        {String("x"), 200},
        {String("z"), 300},
        {std::nullopt, 400},
    };

    auto adapter_serial = Tiforth::runJoinUtf8KeyInt64Payload(
        api,
        Tiforth::PLAN_KIND_INNER_HASH_JOIN_UTF8_KEY_INT64_PAYLOAD,
        build_rows,
        probe_rows,
        1,
        Tiforth::BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        Tiforth::AMBIENT_REQUIREMENT_CHARSET | Tiforth::AMBIENT_REQUIREMENT_DEFAULT_COLLATION,
        Tiforth::SESSION_CHARSET_UTF8MB4,
        Tiforth::DEFAULT_COLLATION_UTF8MB4_BIN);
    auto adapter_parallel = Tiforth::runJoinUtf8KeyInt64Payload(
        api,
        Tiforth::PLAN_KIND_INNER_HASH_JOIN_UTF8_KEY_INT64_PAYLOAD,
        build_rows,
        probe_rows,
        2,
        Tiforth::BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        Tiforth::AMBIENT_REQUIREMENT_CHARSET | Tiforth::AMBIENT_REQUIREMENT_DEFAULT_COLLATION,
        Tiforth::SESSION_CHARSET_UTF8MB4,
        Tiforth::DEFAULT_COLLATION_UTF8MB4_BIN);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);

    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);

    std::cout << "[tiforth-host-v2-inner-join] serial=1 warnings=" << adapter_serial.warning_count
              << " rows=" << adapter_serial.rows.size() << " parallel=2 warnings=" << adapter_parallel.warning_count
              << " rows=" << adapter_parallel.rows.size() << " donor_warnings=" << donor_serial.warning_count
              << " donor_rows=" << donor_serial.rows.size() << " parity=ok" << std::endl;
}

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, InnerHashJoinPayloadParityHighPartitionRetainable)
{
    const bool strict_runtime_execution = Tiforth::requiresStrictRuntimeExecution();
    String load_error;
    auto maybe_api = Tiforth::loadExecutionHostV2Api(load_error);
    if (!maybe_api.has_value())
    {
        if (strict_runtime_execution)
            GTEST_FAIL() << load_error;
        SUCCEED() << load_error;
        return;
    }

    auto api = std::move(maybe_api.value());

    auto donor_serial = runDonorNativeInnerJoin(1);
    auto donor_parallel = runDonorNativeInnerJoin(4);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    const std::vector<Tiforth::Utf8Int64Row> build_rows = {
        {String("k"), 10},
        {String("k"), 20},
        {String("x"), 30},
        {std::nullopt, 40},
    };
    const std::vector<Tiforth::Utf8Int64Row> probe_rows = {
        {String("k"), 100},
        {String("x"), 200},
        {String("z"), 300},
        {std::nullopt, 400},
    };

    auto adapter_serial_many_partitions = Tiforth::runJoinUtf8KeyInt64Payload(
        api,
        Tiforth::PLAN_KIND_INNER_HASH_JOIN_UTF8_KEY_INT64_PAYLOAD,
        build_rows,
        probe_rows,
        8,
        Tiforth::BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        Tiforth::AMBIENT_REQUIREMENT_CHARSET | Tiforth::AMBIENT_REQUIREMENT_DEFAULT_COLLATION,
        Tiforth::SESSION_CHARSET_UTF8MB4,
        Tiforth::DEFAULT_COLLATION_UTF8MB4_BIN);
    auto adapter_parallel_many_partitions = Tiforth::runJoinUtf8KeyInt64Payload(
        api,
        Tiforth::PLAN_KIND_INNER_HASH_JOIN_UTF8_KEY_INT64_PAYLOAD,
        build_rows,
        probe_rows,
        8,
        Tiforth::BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        Tiforth::AMBIENT_REQUIREMENT_CHARSET | Tiforth::AMBIENT_REQUIREMENT_DEFAULT_COLLATION,
        Tiforth::SESSION_CHARSET_UTF8MB4,
        Tiforth::DEFAULT_COLLATION_UTF8MB4_BIN);

    ASSERT_EQ(adapter_serial_many_partitions.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel_many_partitions.warning_count, donor_serial.warning_count);

    ASSERT_EQ(adapter_serial_many_partitions.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel_many_partitions.rows, donor_serial.rows);

    std::cout << "[tiforth-host-v2-inner-join-partitions] serial=8 warnings="
              << adapter_serial_many_partitions.warning_count << " rows=" << adapter_serial_many_partitions.rows.size()
              << " parallel=8 warnings=" << adapter_parallel_many_partitions.warning_count
              << " rows=" << adapter_parallel_many_partitions.rows.size()
              << " donor_warnings=" << donor_serial.warning_count << " donor_rows=" << donor_serial.rows.size()
              << " parity=ok" << std::endl;
}

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, InnerHashJoinPayloadParitySplitOutputSerialAndParallel)
{
    const bool strict_runtime_execution = Tiforth::requiresStrictRuntimeExecution();
    String load_error;
    auto maybe_api = Tiforth::loadExecutionHostV2Api(load_error);
    if (!maybe_api.has_value())
    {
        if (strict_runtime_execution)
            GTEST_FAIL() << load_error;
        SUCCEED() << load_error;
        return;
    }

    auto api = std::move(maybe_api.value());

    auto donor_serial = runDonorNativeInnerJoin(1);
    auto donor_parallel = runDonorNativeInnerJoin(2);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    const std::vector<Tiforth::Utf8Int64Row> build_rows = {
        {String("k"), 10},
        {String("k"), 20},
        {String("x"), 30},
        {std::nullopt, 40},
    };
    const std::vector<Tiforth::Utf8Int64Row> probe_rows = {
        {String("k"), 100},
        {String("x"), 200},
        {String("z"), 300},
        {std::nullopt, 400},
    };

    auto adapter_serial = Tiforth::runJoinUtf8KeyInt64Payload(
        api,
        Tiforth::PLAN_KIND_INNER_HASH_JOIN_UTF8_KEY_INT64_PAYLOAD,
        build_rows,
        probe_rows,
        1,
        Tiforth::BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        Tiforth::AMBIENT_REQUIREMENT_CHARSET | Tiforth::AMBIENT_REQUIREMENT_DEFAULT_COLLATION,
        Tiforth::SESSION_CHARSET_UTF8MB4,
        Tiforth::DEFAULT_COLLATION_UTF8MB4_BIN,
        1);
    auto adapter_parallel = Tiforth::runJoinUtf8KeyInt64Payload(
        api,
        Tiforth::PLAN_KIND_INNER_HASH_JOIN_UTF8_KEY_INT64_PAYLOAD,
        build_rows,
        probe_rows,
        2,
        Tiforth::BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        Tiforth::AMBIENT_REQUIREMENT_CHARSET | Tiforth::AMBIENT_REQUIREMENT_DEFAULT_COLLATION,
        Tiforth::SESSION_CHARSET_UTF8MB4,
        Tiforth::DEFAULT_COLLATION_UTF8MB4_BIN,
        1);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);

    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);

    std::cout << "[tiforth-host-v2-inner-join-split-output] serial=1 warnings=" << adapter_serial.warning_count
              << " rows=" << adapter_serial.rows.size() << " parallel=2 warnings=" << adapter_parallel.warning_count
              << " rows=" << adapter_parallel.rows.size() << " donor_warnings=" << donor_serial.warning_count
              << " donor_rows=" << donor_serial.rows.size() << " max_block_size=1 parity=ok" << std::endl;
}

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, BuildOuterHashJoinPayloadParitySerialAndParallel)
{
    const bool strict_runtime_execution = Tiforth::requiresStrictRuntimeExecution();
    String load_error;
    auto maybe_api = Tiforth::loadExecutionHostV2Api(load_error);
    if (!maybe_api.has_value())
    {
        if (strict_runtime_execution)
            GTEST_FAIL() << load_error;
        SUCCEED() << load_error;
        return;
    }

    auto api = std::move(maybe_api.value());

    auto donor_serial = runDonorNativeBuildOuterJoin(1);
    auto donor_parallel = runDonorNativeBuildOuterJoin(2);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    const std::vector<Tiforth::Int64Int64Row> build_rows = {
        {1, 10},
        {1, 11},
        {5, 50},
        {7, 70},
    };
    const std::vector<Tiforth::Int64Int64Row> probe_rows = {
        {1, 100},
        {5, 500},
    };

    auto adapter_serial = Tiforth::runJoinInt64KeyInt64Payload(
        api,
        Tiforth::PLAN_KIND_BUILD_OUTER_HASH_JOIN_INT64_KEY_INT64_PAYLOAD,
        build_rows,
        probe_rows,
        1,
        Tiforth::BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        0,
        0,
        0);
    auto adapter_parallel = Tiforth::runJoinInt64KeyInt64Payload(
        api,
        Tiforth::PLAN_KIND_BUILD_OUTER_HASH_JOIN_INT64_KEY_INT64_PAYLOAD,
        build_rows,
        probe_rows,
        2,
        Tiforth::BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        0,
        0,
        0);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);

    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);

    std::cout << "[tiforth-host-v2-build-outer-join] serial=1 warnings=" << adapter_serial.warning_count
              << " rows=" << adapter_serial.rows.size() << " parallel=2 warnings=" << adapter_parallel.warning_count
              << " rows=" << adapter_parallel.rows.size() << " donor_warnings=" << donor_serial.warning_count
              << " donor_rows=" << donor_serial.rows.size() << " parity=ok" << std::endl;
}

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, BuildOuterHashJoinPayloadParityLegacyBuildEndSplitOutput)
{
    const bool strict_runtime_execution = Tiforth::requiresStrictRuntimeExecution();
    String load_error;
    auto maybe_api = Tiforth::loadExecutionHostV2Api(load_error);
    if (!maybe_api.has_value())
    {
        if (strict_runtime_execution)
            GTEST_FAIL() << load_error;
        SUCCEED() << load_error;
        return;
    }

    auto api = std::move(maybe_api.value());

    auto donor_serial = runDonorNativeBuildOuterJoin(1);
    auto donor_parallel = runDonorNativeBuildOuterJoin(2);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    const std::vector<Tiforth::Int64Int64Row> build_rows = {
        {1, 10},
        {1, 11},
        {5, 50},
        {7, 70},
    };
    const std::vector<Tiforth::Int64Int64Row> probe_rows = {
        {1, 100},
        {5, 500},
    };

    auto adapter_serial = Tiforth::runJoinInt64KeyInt64Payload(
        api,
        Tiforth::PLAN_KIND_BUILD_OUTER_HASH_JOIN_INT64_KEY_INT64_PAYLOAD,
        build_rows,
        probe_rows,
        1,
        Tiforth::BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        0,
        0,
        0,
        1,
        false);
    auto adapter_parallel = Tiforth::runJoinInt64KeyInt64Payload(
        api,
        Tiforth::PLAN_KIND_BUILD_OUTER_HASH_JOIN_INT64_KEY_INT64_PAYLOAD,
        build_rows,
        probe_rows,
        2,
        Tiforth::BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        0,
        0,
        0,
        1,
        false);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);

    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);

    std::cout << "[tiforth-host-v2-build-outer-join-legacy-end] serial=1 warnings="
              << adapter_serial.warning_count << " rows=" << adapter_serial.rows.size()
              << " parallel=2 warnings=" << adapter_parallel.warning_count
              << " rows=" << adapter_parallel.rows.size() << " donor_warnings=" << donor_serial.warning_count
              << " donor_rows=" << donor_serial.rows.size()
              << " legacy_build_end=1 max_block_size=1 parity=ok" << std::endl;
}

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, ProbeOuterHashJoinPayloadParitySerialAndParallel)
{
    const bool strict_runtime_execution = Tiforth::requiresStrictRuntimeExecution();
    String load_error;
    auto maybe_api = Tiforth::loadExecutionHostV2Api(load_error);
    if (!maybe_api.has_value())
    {
        if (strict_runtime_execution)
            GTEST_FAIL() << load_error;
        SUCCEED() << load_error;
        return;
    }

    auto api = std::move(maybe_api.value());

    auto donor_serial = runDonorNativeProbeOuterJoin(1);
    auto donor_parallel = runDonorNativeProbeOuterJoin(2);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    const std::vector<Tiforth::Int64Int64Row> build_rows = {
        {1, 10},
        {1, 11},
        {5, 50},
    };
    const std::vector<Tiforth::Int64Int64Row> probe_rows = {
        {1, 100},
        {2, 200},
        {std::nullopt, 300},
        {5, 500},
    };

    auto adapter_serial = Tiforth::runJoinInt64KeyInt64Payload(
        api,
        Tiforth::PLAN_KIND_PROBE_OUTER_HASH_JOIN_INT64_KEY_INT64_PAYLOAD,
        build_rows,
        probe_rows,
        1,
        Tiforth::BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        0,
        0,
        0);
    auto adapter_parallel = Tiforth::runJoinInt64KeyInt64Payload(
        api,
        Tiforth::PLAN_KIND_PROBE_OUTER_HASH_JOIN_INT64_KEY_INT64_PAYLOAD,
        build_rows,
        probe_rows,
        2,
        Tiforth::BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        0,
        0,
        0);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);

    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);

    std::cout << "[tiforth-host-v2-probe-outer-join] serial=1 warnings=" << adapter_serial.warning_count
              << " rows=" << adapter_serial.rows.size() << " parallel=2 warnings=" << adapter_parallel.warning_count
              << " rows=" << adapter_parallel.rows.size() << " donor_warnings=" << donor_serial.warning_count
              << " donor_rows=" << donor_serial.rows.size() << " parity=ok" << std::endl;
}

} // namespace
} // namespace DB::tests
