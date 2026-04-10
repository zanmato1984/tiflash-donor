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

#include <TestUtils/FunctionTestUtils.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <fmt/core.h>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

namespace DB::tests
{
namespace
{

constexpr uint32_t EXECUTION_HOST_V2_ABI_VERSION = 4;
constexpr uint32_t PLAN_KIND_CAST_UTF8_TO_DECIMAL = 1;
constexpr uint32_t INPUT_ID_SCALAR = 0;
constexpr uint32_t SQL_MODE_TRUNCATE_AS_WARNING = 2;
constexpr uint32_t STATUS_KIND_OK = 0;
constexpr uint32_t STATUS_KIND_PROTOCOL_ERROR = 5;
constexpr uint32_t STATUS_CODE_NONE = 0;
constexpr uint32_t STATUS_CODE_INSTANCE_FINISH_ONLY = 13;
constexpr uint32_t PHYSICAL_TYPE_UTF8 = 2;
constexpr uint32_t PHYSICAL_TYPE_DECIMAL128 = 4;
constexpr uint32_t BATCH_OWNERSHIP_BORROW_WITHIN_CALL = 1;
constexpr uint32_t BATCH_OWNERSHIP_FOREIGN_RETAINABLE = 2;
constexpr uint32_t AMBIENT_REQUIREMENT_SQL_MODE = 1u << 0;

constexpr const char * FUNC_NAME_TIDB_CAST = "tidb_cast";

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
#error "build gtests_dbms with -DENABLE_TIFORTH_HOST_V2_LINKED_TESTS=ON and linked libtiforth_ffi_c input"
#endif

bool isValidRow(const TiforthExecutionColumnViewV2 & column, uint32_t row_count, size_t row)
{
    if (column.null_bitmap == nullptr || row_count == 0)
        return true;
    const size_t bit_index = static_cast<size_t>(column.null_bitmap_bit_offset) + row;
    return (column.null_bitmap[bit_index / 8] & (1u << (bit_index % 8))) != 0;
}

String uint128ToString(unsigned __int128 value)
{
    if (value == 0)
        return "0";

    String digits;
    while (value > 0)
    {
        const uint8_t digit = static_cast<uint8_t>(value % 10);
        digits.push_back(static_cast<char>('0' + digit));
        value /= 10;
    }
    std::reverse(digits.begin(), digits.end());
    return digits;
}

String formatDecimal128(__int128 value, int8_t scale)
{
    const bool negative = value < 0;
    unsigned __int128 abs_value;
    if (negative)
    {
        // avoid overflow when value is the smallest signed 128-bit number
        abs_value = static_cast<unsigned __int128>(-(value + 1));
        abs_value += 1;
    }
    else
    {
        abs_value = static_cast<unsigned __int128>(value);
    }

    if (scale == 0)
    {
        String rendered = uint128ToString(abs_value);
        if (negative)
            rendered.insert(rendered.begin(), '-');
        return rendered;
    }

    String digits = uint128ToString(abs_value);
    const size_t scale_digits = static_cast<size_t>(scale);
    if (digits.size() <= scale_digits)
        digits.insert(0, scale_digits + 1 - digits.size(), '0');

    const size_t split = digits.size() - scale_digits;
    String rendered = fmt::format("{}.{}", digits.substr(0, split), digits.substr(split));
    if (negative)
        rendered.insert(rendered.begin(), '-');
    return rendered;
}

struct Utf8BatchOwned
{
    std::vector<uint8_t> null_bitmap;
    std::vector<int32_t> offsets;
    String data;
    TiforthExecutionColumnViewV2 column{};
    TiforthBatchViewV2 batch{};

    Utf8BatchOwned(const std::vector<std::optional<String>> & rows, uint32_t ownership_mode)
    {
        null_bitmap.assign(rows.size() == 0 ? 0 : (rows.size() + 7) / 8, 0);
        offsets.reserve(rows.size() + 1);
        offsets.push_back(0);

        for (size_t i = 0; i < rows.size(); ++i)
        {
            if (rows[i].has_value())
            {
                null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
                data.append(rows[i].value());
            }
            offsets.push_back(static_cast<int32_t>(data.size()));
        }

        column.physical_type = PHYSICAL_TYPE_UTF8;
        column.null_bitmap = null_bitmap.empty() ? nullptr : null_bitmap.data();
        column.null_bitmap_bit_offset = 0;
        column.row_offset = 0;
        column.values = nullptr;
        column.offsets = offsets.data();
        column.data = reinterpret_cast<const uint8_t *>(data.data());
        column.decimal128_words = nullptr;
        column.decimal_precision_is_set = false;
        column.decimal_precision = 0;
        column.decimal_scale_is_set = false;
        column.decimal_scale = 0;

        batch.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        batch.ownership_mode = ownership_mode;
        batch.column_count = 1;
        batch.row_count = static_cast<uint32_t>(rows.size());
        batch.columns = &column;
    }
};

struct AdapterRunResult
{
    std::vector<std::optional<String>> output;
    uint32_t warning_count = 0;
};

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

    void runAdapterCast(
        const std::vector<std::optional<String>> & input,
        size_t partitions,
        uint32_t ownership_mode,
        AdapterRunResult & result)
    {
        TiforthExecutionBuildRequestV2 build_request{};
        build_request.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        build_request.plan_kind = PLAN_KIND_CAST_UTF8_TO_DECIMAL;
        build_request.ambient_requirement_mask = AMBIENT_REQUIREMENT_SQL_MODE;
        build_request.sql_mode = SQL_MODE_TRUNCATE_AS_WARNING;
        build_request.session_charset = 0;
        build_request.default_collation = 0;
        build_request.decimal_precision_is_set = true;
        build_request.decimal_precision = 10;
        build_request.decimal_scale_is_set = true;
        build_request.decimal_scale = 3;
        build_request.max_block_size = 0;

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

        result.output.clear();
        result.warning_count = 0;
        const size_t chunk_size = std::max<size_t>(1, (input.size() + partitions - 1) / partitions);
        std::vector<Utf8BatchOwned> retained_batches;
        if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
        {
            // Retainable ownership requires input buffers to stay alive until finish().
            const size_t batch_count = input.empty() ? 0 : (input.size() + chunk_size - 1) / chunk_size;
            retained_batches.reserve(batch_count);
        }

        for (size_t start = 0; start < input.size(); start += chunk_size)
        {
            const size_t end = std::min(input.size(), start + chunk_size);
            std::vector<std::optional<String>> batch_rows(input.begin() + static_cast<ptrdiff_t>(start), input.begin() + static_cast<ptrdiff_t>(end));
            std::optional<Utf8BatchOwned> batch;
            const TiforthBatchViewV2 * input_batch = nullptr;
            if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
            {
                retained_batches.emplace_back(batch_rows, ownership_mode);
                input_batch = &retained_batches.back().batch;
            }
            else
            {
                batch.emplace(batch_rows, ownership_mode);
                input_batch = &batch->batch;
            }

            TiforthBatchViewV2 output{};
            output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
            tiforth_execution_host_v2_drive_input_batch(instance, INPUT_ID_SCALAR, input_batch, &status, &output);

            ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
            ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
            result.warning_count += status.warning_count;

            ASSERT_EQ(output.column_count, 1u);
            const auto & output_column = output.columns[0];
            ASSERT_EQ(output_column.physical_type, PHYSICAL_TYPE_DECIMAL128);

            const auto * decimal_values = reinterpret_cast<const __int128 *>(output_column.decimal128_words);
            ASSERT_NE(decimal_values, nullptr);
            decimal_values += output_column.row_offset;
            for (size_t row = 0; row < output.row_count; ++row)
            {
                if (!isValidRow(output_column, output.row_count, row))
                {
                    result.output.push_back(std::nullopt);
                    continue;
                }
                result.output.push_back(formatDecimal128(decimal_values[row], output_column.decimal_scale));
            }
        }

        tiforth_execution_host_v2_drive_end_of_input(instance, INPUT_ID_SCALAR, &status);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;

        TiforthBatchViewV2 continued_output{};
        continued_output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        tiforth_execution_host_v2_continue_output(instance, &status, &continued_output);
        ASSERT_EQ(continued_output.row_count, 0u);
        ASSERT_EQ(continued_output.column_count, 0u);
        ASSERT_EQ(status.kind, STATUS_KIND_PROTOCOL_ERROR) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_INSTANCE_FINISH_ONLY) << status.message;

        tiforth_execution_host_v2_finish(instance, &status);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;

        tiforth_execution_host_v2_release_instance(instance);
        tiforth_execution_host_v2_release_executable(executable);
    }
};

TEST_F(TestTiforthExecutionHostV2Cast, CastUtf8ToDecimalParitySerialAndParallel)
{
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
    const auto donor_warning_count = dag_context.getWarningCount();

    AdapterRunResult serial;
    runAdapterCast(input, 1, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, serial);
    AdapterRunResult parallel;
    runAdapterCast(input, 2, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, parallel);

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

    AdapterRunResult serial;
    runAdapterCast(input, 1, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, serial);
    AdapterRunResult parallel;
    runAdapterCast(input, 2, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, parallel);

    ASSERT_EQ(serial.warning_count, donor_warning_count);
    ASSERT_EQ(parallel.warning_count, donor_warning_count);

    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(serial.output), donor_native);
    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(parallel.output), donor_native);

    std::cout << "[tiforth-host-v2-cast-scale-loss] serial=1 warnings=" << serial.warning_count
              << " rows=" << serial.output.size() << " parallel=2 warnings=" << parallel.warning_count
              << " rows=" << parallel.output.size() << " donor_warnings=" << donor_warning_count << " parity=ok"
              << std::endl;
}

TEST_F(TestTiforthExecutionHostV2Cast, CastUtf8ToDecimalMalformedMultiDotZeroParitySerialAndParallel)
{
    auto & dag_context = getDAGContext();
    ScopedDAGFlags scoped_dag_flags(dag_context);
    dag_context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);

    const std::vector<std::optional<String>> input = {
        String("1.2.3"),
        String("bad"),
        String("12.340"),
        String("-0.500"),
        std::nullopt,
        String(""),
    };

    auto donor_native = runDonorNativeCastAsString(input);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({
            String("0.000"),
            String("0.000"),
            String("12.340"),
            String("-0.500"),
            std::nullopt,
            String("0.000"),
        }),
        donor_native);
    const auto donor_warning_count = static_cast<uint32_t>(dag_context.getWarningCount());
    ASSERT_EQ(donor_warning_count, 0u);

    AdapterRunResult serial;
    runAdapterCast(input, 1, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, serial);
    AdapterRunResult parallel;
    runAdapterCast(input, 2, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, parallel);

    ASSERT_EQ(serial.warning_count, donor_warning_count);
    ASSERT_EQ(parallel.warning_count, donor_warning_count);

    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(serial.output), donor_native);
    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(parallel.output), donor_native);

    std::cout << "[tiforth-host-v2-cast-multi-dot] serial=1 warnings=" << serial.warning_count
              << " rows=" << serial.output.size() << " parallel=2 warnings=" << parallel.warning_count
              << " rows=" << parallel.output.size() << " donor_warnings=" << donor_warning_count << " parity=ok"
              << std::endl;
}

TEST_F(TestTiforthExecutionHostV2Cast, CastUtf8ToDecimalMalformedSignedMultiDotZeroParitySerialAndParallel)
{
    auto & dag_context = getDAGContext();
    ScopedDAGFlags scoped_dag_flags(dag_context);
    dag_context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);

    const std::vector<std::optional<String>> input = {
        String("-1.2.3"),
        String("+1.2.3"),
        String("-bad"),
        String("+"),
        String("-0.500"),
        std::nullopt,
    };

    auto donor_native = runDonorNativeCastAsString(input);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({
            String("0.000"),
            String("0.000"),
            String("0.000"),
            String("0.000"),
            String("-0.500"),
            std::nullopt,
        }),
        donor_native);
    const auto donor_warning_count = static_cast<uint32_t>(dag_context.getWarningCount());
    ASSERT_EQ(donor_warning_count, 0u);

    AdapterRunResult serial;
    runAdapterCast(input, 1, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, serial);
    AdapterRunResult parallel;
    runAdapterCast(input, 2, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, parallel);

    ASSERT_EQ(serial.warning_count, donor_warning_count);
    ASSERT_EQ(parallel.warning_count, donor_warning_count);

    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(serial.output), donor_native);
    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(parallel.output), donor_native);

    std::cout << "[tiforth-host-v2-cast-signed-multi-dot] serial=1 warnings=" << serial.warning_count
              << " rows=" << serial.output.size() << " parallel=2 warnings=" << parallel.warning_count
              << " rows=" << parallel.output.size() << " donor_warnings=" << donor_warning_count << " parity=ok"
              << std::endl;
}

TEST_F(TestTiforthExecutionHostV2Cast, CastUtf8ToDecimalInvalidSyntaxWarningParitySerialAndParallel)
{
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

    AdapterRunResult serial;
    runAdapterCast(input, 1, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, serial);
    AdapterRunResult parallel;
    runAdapterCast(input, 2, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, parallel);

    ASSERT_EQ(serial.warning_count, donor_warning_count);
    ASSERT_EQ(parallel.warning_count, donor_warning_count);

    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(serial.output), donor_native);
    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(parallel.output), donor_native);

    std::cout << "[tiforth-host-v2-cast-invalid-syntax] serial=1 warnings=" << serial.warning_count
              << " rows=" << serial.output.size() << " parallel=2 warnings=" << parallel.warning_count
              << " rows=" << parallel.output.size() << " donor_warnings=" << donor_warning_count << " parity=ok"
              << std::endl;
}

} // namespace
} // namespace DB::tests
