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

#include <Functions/TiforthExecutionHostV2Adapter.h>
#include <TestUtils/FunctionTestUtils.h>
#include <gtest/gtest.h>

#include <iostream>

namespace DB::tests
{
namespace
{

constexpr const char * FUNC_NAME_TIDB_CAST = "tidb_cast";

class ScopedDAGFlags
{
public:
    explicit ScopedDAGFlags(DAGContext & dag_context_)
        : dag_context(dag_context_)
        , original_flags(dag_context_.getFlags())
    {}

    ~ScopedDAGFlags() { dag_context.setFlags(original_flags); }

private:
    DAGContext & dag_context;
    UInt64 original_flags;
};

class TestTiforthExecutionHostV2Cast : public FunctionTest
{
public:
    ColumnWithTypeAndName runDonorNativeCastAsString(const std::vector<std::optional<String>> & input)
    {
        getDAGContext().clearWarnings();
        auto input_column = createColumn<Nullable<String>>(input);
        auto decimal_column = executeFunction(
            FUNC_NAME_TIDB_CAST,
            {input_column, createConstColumn<String>(1, "Nullable(Decimal(10,3))")});
        return executeFunction(
            FUNC_NAME_TIDB_CAST,
            {decimal_column, createConstColumn<String>(1, "Nullable(String)")});
    }
};

TEST_F(TestTiforthExecutionHostV2Cast, CastUtf8ToDecimalParitySerialAndParallel)
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
    auto & dag_context = getDAGContext();
    ScopedDAGFlags scoped_dag_flags(dag_context);
    dag_context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);

    const std::vector<std::optional<String>> input = {
        String("12.345"),
        String("-7.800"),
        std::nullopt,
        String("0"),
        String("999.999"),
        String("1.2"),
    };

    auto donor_native = runDonorNativeCastAsString(input);
    const auto donor_warning_count = static_cast<uint32_t>(dag_context.getWarningCount());

    auto serial = Tiforth::runCastUtf8ToDecimal(
        api,
        input,
        1,
        Tiforth::BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        Tiforth::SQL_MODE_TRUNCATE_AS_WARNING,
        10,
        3);
    auto parallel = Tiforth::runCastUtf8ToDecimal(
        api,
        input,
        2,
        Tiforth::BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        Tiforth::SQL_MODE_TRUNCATE_AS_WARNING,
        10,
        3);

    ASSERT_EQ(serial.warning_count, donor_warning_count);
    ASSERT_EQ(parallel.warning_count, donor_warning_count);

    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(serial.output), donor_native);
    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(parallel.output), donor_native);

    std::cout << "[tiforth-host-v2-cast] serial=1 warnings=" << serial.warning_count << " rows=" << serial.output.size()
              << " parallel=2 warnings=" << parallel.warning_count << " rows=" << parallel.output.size()
              << " donor_warnings=" << donor_warning_count << " parity=ok" << std::endl;
}

TEST_F(TestTiforthExecutionHostV2Cast, CastUtf8ToDecimalScaleLossWarningParitySerialAndParallel)
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
    auto & dag_context = getDAGContext();
    ScopedDAGFlags scoped_dag_flags(dag_context);
    dag_context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);

    const std::vector<std::optional<String>> input = {
        String("1.2395"),
        String("-7.8001"),
        std::nullopt,
        String("999.9999"),
        String("0.0004"),
    };

    auto donor_native = runDonorNativeCastAsString(input);
    const auto donor_warning_count = static_cast<uint32_t>(dag_context.getWarningCount());
    ASSERT_GT(donor_warning_count, 0u);

    auto serial = Tiforth::runCastUtf8ToDecimal(
        api,
        input,
        1,
        Tiforth::BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        Tiforth::SQL_MODE_TRUNCATE_AS_WARNING,
        10,
        3);
    auto parallel = Tiforth::runCastUtf8ToDecimal(
        api,
        input,
        2,
        Tiforth::BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        Tiforth::SQL_MODE_TRUNCATE_AS_WARNING,
        10,
        3);

    ASSERT_EQ(serial.warning_count, donor_warning_count);
    ASSERT_EQ(parallel.warning_count, donor_warning_count);

    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(serial.output), donor_native);
    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(parallel.output), donor_native);

    std::cout << "[tiforth-host-v2-cast-scale-loss] serial=1 warnings=" << serial.warning_count
              << " rows=" << serial.output.size() << " parallel=2 warnings=" << parallel.warning_count
              << " rows=" << parallel.output.size() << " donor_warnings=" << donor_warning_count << " parity=ok"
              << std::endl;
}

TEST_F(TestTiforthExecutionHostV2Cast, CastUtf8ToDecimalInvalidSyntaxWarningParitySerialAndParallel)
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
    auto & dag_context = getDAGContext();
    ScopedDAGFlags scoped_dag_flags(dag_context);
    dag_context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);

    const std::vector<std::optional<String>> input = {
        String("bad"),
        String("1.2.3"),
        String("12.340"),
        String("-0.500"),
        std::nullopt,
        String(""),
    };

    auto donor_native = runDonorNativeCastAsString(input);
    const auto donor_warning_count = static_cast<uint32_t>(dag_context.getWarningCount());

    auto serial = Tiforth::runCastUtf8ToDecimal(
        api,
        input,
        1,
        Tiforth::BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        Tiforth::SQL_MODE_TRUNCATE_AS_WARNING,
        10,
        3);
    auto parallel = Tiforth::runCastUtf8ToDecimal(
        api,
        input,
        2,
        Tiforth::BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        Tiforth::SQL_MODE_TRUNCATE_AS_WARNING,
        10,
        3);

    ASSERT_EQ(serial.warning_count, donor_warning_count);
    ASSERT_EQ(parallel.warning_count, donor_warning_count);

    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(serial.output), donor_native);
    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(parallel.output), donor_native);

    std::cout << "[tiforth-host-v2-cast-invalid-syntax] serial=1 warnings=" << serial.warning_count
              << " rows=" << serial.output.size() << " parallel=2 warnings=" << parallel.warning_count
              << " rows=" << parallel.output.size() << " donor_warnings=" << donor_warning_count << " parity=ok"
              << std::endl;
}

TEST_F(TestTiforthExecutionHostV2Cast, CastUtf8ToDecimalInvalidSyntaxWarningParitySplitOutputSerialAndParallel)
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
    auto & dag_context = getDAGContext();
    ScopedDAGFlags scoped_dag_flags(dag_context);
    dag_context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);

    const std::vector<std::optional<String>> input = {
        String("bad"),
        String("1.2.3"),
        String("12.340"),
        String("-0.500"),
        std::nullopt,
        String(""),
    };

    auto donor_native = runDonorNativeCastAsString(input);
    const auto donor_warning_count = static_cast<uint32_t>(dag_context.getWarningCount());
    ASSERT_EQ(donor_warning_count, 0u);

    auto serial = Tiforth::runCastUtf8ToDecimal(
        api,
        input,
        1,
        Tiforth::BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        Tiforth::SQL_MODE_TRUNCATE_AS_WARNING,
        10,
        3,
        1);
    auto parallel = Tiforth::runCastUtf8ToDecimal(
        api,
        input,
        8,
        Tiforth::BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        Tiforth::SQL_MODE_TRUNCATE_AS_WARNING,
        10,
        3,
        1);

    ASSERT_EQ(serial.warning_count, donor_warning_count);
    ASSERT_EQ(parallel.warning_count, donor_warning_count);

    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(serial.output), donor_native);
    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(parallel.output), donor_native);

    std::cout << "[tiforth-host-v2-cast-invalid-syntax-split-output] serial=1 warnings=" << serial.warning_count
              << " rows=" << serial.output.size() << " parallel=8 warnings=" << parallel.warning_count
              << " rows=" << parallel.output.size() << " donor_warnings=" << donor_warning_count
              << " max_block_size=1 parity=ok" << std::endl;
}

TEST_F(TestTiforthExecutionHostV2Cast, CastUtf8ToDecimalScaleLossWarningParitySplitOutputSerialAndParallel)
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
    auto & dag_context = getDAGContext();
    ScopedDAGFlags scoped_dag_flags(dag_context);
    dag_context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);

    const std::vector<std::optional<String>> input = {
        String("1.2395"),
        String("-7.8001"),
        std::nullopt,
        String("999.9999"),
        String("0.0004"),
        String("-0.0005"),
        String("12345.6789"),
        String("7.0006"),
        std::nullopt,
        String("-999.9999"),
    };

    auto donor_native = runDonorNativeCastAsString(input);
    const auto donor_warning_count = static_cast<uint32_t>(dag_context.getWarningCount());
    ASSERT_GT(donor_warning_count, 0u);

    auto serial = Tiforth::runCastUtf8ToDecimal(
        api,
        input,
        1,
        Tiforth::BATCH_OWNERSHIP_BORROW_WITHIN_CALL,
        Tiforth::SQL_MODE_TRUNCATE_AS_WARNING,
        10,
        3,
        1);
    auto parallel = Tiforth::runCastUtf8ToDecimal(
        api,
        input,
        8,
        Tiforth::BATCH_OWNERSHIP_FOREIGN_RETAINABLE,
        Tiforth::SQL_MODE_TRUNCATE_AS_WARNING,
        10,
        3,
        1);

    ASSERT_EQ(serial.warning_count, donor_warning_count);
    ASSERT_EQ(parallel.warning_count, donor_warning_count);

    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(serial.output), donor_native);
    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(parallel.output), donor_native);

    std::cout << "[tiforth-host-v2-cast-scale-loss-split-output] serial=1 warnings=" << serial.warning_count
              << " rows=" << serial.output.size() << " parallel=8 warnings=" << parallel.warning_count
              << " rows=" << parallel.output.size() << " donor_warnings=" << donor_warning_count
              << " max_block_size=1 parity=ok" << std::endl;
}

} // namespace
} // namespace DB::tests
