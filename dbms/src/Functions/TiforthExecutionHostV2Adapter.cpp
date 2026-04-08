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

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <fmt/core.h>
#include <stdexcept>
#include <utility>

namespace DB::Tiforth
{
namespace
{

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

String statusMessage(const TiforthStatusV2 & status)
{
    const size_t msg_len = strnlen(status.message, sizeof(status.message));
    return String(status.message, msg_len);
}

[[noreturn]] void throwStatusError(const char * step, const TiforthStatusV2 & status)
{
    throw std::runtime_error(
        fmt::format(
            "{} failed: kind={} code={} warning_count={} message={}",
            step,
            status.kind,
            status.code,
            status.warning_count,
            statusMessage(status)));
}

void ensureStatusKindOk(const char * step, const TiforthStatusV2 & status)
{
    if (status.kind != STATUS_KIND_OK)
        throwStatusError(step, status);
}

void ensureStatusOk(const char * step, const TiforthStatusV2 & status)
{
    ensureStatusKindOk(step, status);
    if (status.code != STATUS_CODE_NONE)
        throwStatusError(step, status);
}

TiforthStatusV2 makeStatus()
{
    TiforthStatusV2 status{};
    status.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
    return status;
}

TiforthBatchViewV2 makeBatch()
{
    TiforthBatchViewV2 batch{};
    batch.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
    return batch;
}

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

void appendCastOutputRows(const TiforthBatchViewV2 & output, std::vector<std::optional<String>> & rows)
{
    if (output.row_count == 0)
        return;

    if (output.column_count != 1)
    {
        throw std::runtime_error(
            fmt::format("cast output has {} columns, expected 1", output.column_count));
    }
    if (output.columns == nullptr)
        throw std::runtime_error("cast output columns is null");

    const auto & output_column = output.columns[0];
    if (output_column.physical_type != PHYSICAL_TYPE_DECIMAL128)
    {
        throw std::runtime_error(
            fmt::format(
                "cast output physical_type={}, expected decimal128",
                output_column.physical_type));
    }

    const auto * decimal_values = reinterpret_cast<const __int128 *>(output_column.decimal128_words);
    if (decimal_values == nullptr)
        throw std::runtime_error("cast output decimal128_words is null");

    decimal_values += output_column.row_offset;
    for (size_t row = 0; row < output.row_count; ++row)
    {
        if (!isValidRow(output_column, output.row_count, row))
        {
            rows.push_back(std::nullopt);
            continue;
        }
        rows.push_back(formatDecimal128(decimal_values[row], output_column.decimal_scale));
    }
}

void appendJoinOutputRows(const TiforthBatchViewV2 & output, std::vector<JoinOutputRow> & rows)
{
    if (output.row_count == 0)
        return;

    if (output.column_count != 2)
    {
        throw std::runtime_error(
            fmt::format("join output has {} columns, expected 2", output.column_count));
    }
    if (output.columns == nullptr)
        throw std::runtime_error("join output columns is null");

    const auto & build_payload = output.columns[0];
    const auto & probe_payload = output.columns[1];

    if (build_payload.physical_type != PHYSICAL_TYPE_INT64 || probe_payload.physical_type != PHYSICAL_TYPE_INT64)
    {
        throw std::runtime_error(
            fmt::format(
                "join output physical types are [{}, {}], expected [int64, int64]",
                build_payload.physical_type,
                probe_payload.physical_type));
    }

    if (build_payload.values == nullptr || probe_payload.values == nullptr)
        throw std::runtime_error("join output value pointer is null");

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

struct Utf8BatchOwned
{
    std::vector<uint8_t> null_bitmap;
    std::vector<int32_t> offsets;
    String data;
    TiforthExecutionColumnViewV2 column{};
    TiforthBatchViewV2 batch{};

    Utf8BatchOwned(
        const std::vector<std::optional<String>> & rows,
        size_t start,
        size_t end,
        uint32_t ownership_mode)
    {
        const size_t row_count = end - start;
        null_bitmap.assign(row_count == 0 ? 0 : (row_count + 7) / 8, 0);
        offsets.reserve(row_count + 1);
        offsets.push_back(0);

        for (size_t row = start, i = 0; row < end; ++row, ++i)
        {
            if (rows[row].has_value())
            {
                null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
                data.append(rows[row].value());
            }
            offsets.push_back(static_cast<int32_t>(data.size()));
        }

        column.physical_type = PHYSICAL_TYPE_UTF8;
        column.null_bitmap = null_bitmap.empty() ? nullptr : null_bitmap.data();
        column.null_bitmap_bit_offset = 0;
        column.row_offset = 0;
        column.values = nullptr;
        column.offsets = offsets.data();
        column.data = data.empty() ? nullptr : reinterpret_cast<const uint8_t *>(data.data());
        column.decimal128_words = nullptr;
        column.decimal_precision_is_set = false;
        column.decimal_precision = 0;
        column.decimal_scale_is_set = false;
        column.decimal_scale = 0;

        batch.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        batch.ownership_mode = ownership_mode;
        batch.column_count = 1;
        batch.row_count = static_cast<uint32_t>(row_count);
        batch.columns = &column;
    }
};

struct Utf8Int64JoinBatchOwned
{
    std::vector<uint8_t> key_null_bitmap;
    std::vector<int32_t> key_offsets;
    String key_data;

    std::vector<int64_t> payload_values;
    std::vector<uint8_t> payload_null_bitmap;

    TiforthExecutionColumnViewV2 columns[2]{};
    TiforthBatchViewV2 batch{};

    Utf8Int64JoinBatchOwned(
        const std::vector<Utf8Int64Row> & rows,
        size_t start,
        size_t end,
        uint32_t ownership_mode)
    {
        const size_t row_count = end - start;
        key_null_bitmap.assign(row_count == 0 ? 0 : (row_count + 7) / 8, 0);
        key_offsets.reserve(row_count + 1);
        key_offsets.push_back(0);

        payload_values.reserve(row_count);
        payload_null_bitmap.assign(row_count == 0 ? 0 : (row_count + 7) / 8, 0);

        for (size_t row = start, i = 0; row < end; ++row, ++i)
        {
            if (rows[row].first.has_value())
            {
                key_null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
                key_data.append(rows[row].first.value());
            }
            key_offsets.push_back(static_cast<int32_t>(key_data.size()));

            if (rows[row].second.has_value())
            {
                payload_null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
                payload_values.push_back(rows[row].second.value());
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
        batch.row_count = static_cast<uint32_t>(row_count);
        batch.columns = columns;
    }
};

struct Int64Int64JoinBatchOwned
{
    std::vector<int64_t> key_values;
    std::vector<uint8_t> key_null_bitmap;

    std::vector<int64_t> payload_values;
    std::vector<uint8_t> payload_null_bitmap;

    TiforthExecutionColumnViewV2 columns[2]{};
    TiforthBatchViewV2 batch{};

    Int64Int64JoinBatchOwned(
        const std::vector<Int64Int64Row> & rows,
        size_t start,
        size_t end,
        uint32_t ownership_mode)
    {
        const size_t row_count = end - start;
        key_values.reserve(row_count);
        key_null_bitmap.assign(row_count == 0 ? 0 : (row_count + 7) / 8, 0);

        payload_values.reserve(row_count);
        payload_null_bitmap.assign(row_count == 0 ? 0 : (row_count + 7) / 8, 0);

        for (size_t row = start, i = 0; row < end; ++row, ++i)
        {
            if (rows[row].first.has_value())
            {
                key_null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
                key_values.push_back(rows[row].first.value());
            }
            else
            {
                key_values.push_back(0);
            }

            if (rows[row].second.has_value())
            {
                payload_null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
                payload_values.push_back(rows[row].second.value());
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
        batch.row_count = static_cast<uint32_t>(row_count);
        batch.columns = columns;
    }
};

class ExecutionHandles
{
public:
    explicit ExecutionHandles(const TiforthExecutionHostV2Api & api_)
        : api(api_)
    {}

    ExecutionHandles(const ExecutionHandles &) = delete;
    ExecutionHandles & operator=(const ExecutionHandles &) = delete;

    ~ExecutionHandles()
    {
        if (instance != nullptr)
            api.release_instance(instance);
        if (executable != nullptr)
            api.release_executable(executable);
    }

    TiforthExecutionInstanceHandleV2 * buildAndOpen(const TiforthExecutionBuildRequestV2 & request)
    {
        auto status = makeStatus();
        api.build(&request, &status, &executable);
        ensureStatusOk("build", status);
        if (executable == nullptr)
            throw std::runtime_error("build returned null executable");

        status = makeStatus();
        api.open(executable, &status, &instance);
        ensureStatusOk("open", status);
        if (instance == nullptr)
            throw std::runtime_error("open returned null instance");

        return instance;
    }

private:
    const TiforthExecutionHostV2Api & api;
    TiforthExecutionExecutableHandleV2 * executable = nullptr;
    TiforthExecutionInstanceHandleV2 * instance = nullptr;
};

void drainJoinOutput(
    const TiforthExecutionHostV2Api & api,
    TiforthExecutionInstanceHandleV2 * instance,
    TiforthStatusV2 & status,
    JoinRunResult & result)
{
    while (status.code == STATUS_CODE_MORE_OUTPUT_AVAILABLE)
    {
        auto continued_output = makeBatch();
        status = makeStatus();
        api.continue_output(instance, &status, &continued_output);
        ensureStatusKindOk("continue_output", status);
        result.warning_count += status.warning_count;
        appendJoinOutputRows(continued_output, result.rows);
    }

    if (status.code != STATUS_CODE_NONE)
    {
        throw std::runtime_error(
            fmt::format("join produced unexpected status code {}", status.code));
    }
}

} // namespace

std::optional<TiforthExecutionHostV2Api> loadExecutionHostV2Api(String & error)
{
#if defined(TIFORTH_HOST_V2_LINKED_TESTS)
    (void)error;

    TiforthExecutionHostV2Api api;
    api.build = tiforth_execution_host_v2_build;
    api.open = tiforth_execution_host_v2_open;
    api.drive_input_batch = tiforth_execution_host_v2_drive_input_batch;
    api.drive_end_of_input = tiforth_execution_host_v2_drive_end_of_input;
    api.drive_end_of_input_with_output = tiforth_execution_host_v2_drive_end_of_input_with_output;
    api.continue_output = tiforth_execution_host_v2_continue_output;
    api.finish = tiforth_execution_host_v2_finish;
    api.release_executable = tiforth_execution_host_v2_release_executable;
    api.release_instance = tiforth_execution_host_v2_release_instance;
    return api;
#else
    error
        = "build gtests_dbms with -DENABLE_TIFORTH_HOST_V2_LINKED_TESTS=ON "
          "-DTIFORTH_FFI_C_LIBRARY=/abs/path/to/libtiforth_ffi_c.<so|dylib> to run this donor adapter test";
    return std::nullopt;
#endif
}

bool requiresStrictRuntimeExecution()
{
    const char * configured = std::getenv("TIFORTH_REQUIRE_RUNTIME_EXECUTION");
    return configured != nullptr && configured[0] != '\0' && configured[0] != '0';
}

CastRunResult runCastUtf8ToDecimal(
    const TiforthExecutionHostV2Api & api,
    const std::vector<std::optional<String>> & input,
    size_t partitions,
    uint32_t ownership_mode,
    uint32_t sql_mode,
    uint8_t decimal_precision,
    int8_t decimal_scale)
{
    TiforthExecutionBuildRequestV2 build_request{};
    build_request.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
    build_request.plan_kind = PLAN_KIND_CAST_UTF8_TO_DECIMAL;
    build_request.ambient_requirement_mask = AMBIENT_REQUIREMENT_SQL_MODE;
    build_request.sql_mode = sql_mode;
    build_request.session_charset = 0;
    build_request.default_collation = 0;
    build_request.decimal_precision_is_set = true;
    build_request.decimal_precision = decimal_precision;
    build_request.decimal_scale_is_set = true;
    build_request.decimal_scale = decimal_scale;
    build_request.max_block_size = 0;

    ExecutionHandles handles(api);
    TiforthExecutionInstanceHandleV2 * instance = handles.buildAndOpen(build_request);

    CastRunResult result;
    const size_t partition_count = std::max<size_t>(1, partitions);
    const size_t chunk_size = std::max<size_t>(1, (input.size() + partition_count - 1) / partition_count);
    std::vector<Utf8BatchOwned> retained_batches;
    if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
    {
        const size_t batch_count = input.empty() ? 0 : (input.size() + chunk_size - 1) / chunk_size;
        retained_batches.reserve(batch_count);
    }

    for (size_t start = 0; start < input.size(); start += chunk_size)
    {
        const size_t end = std::min(input.size(), start + chunk_size);
        const TiforthBatchViewV2 * input_batch = nullptr;
        std::optional<Utf8BatchOwned> borrowed_batch;
        if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
        {
            retained_batches.emplace_back(input, start, end, ownership_mode);
            input_batch = &retained_batches.back().batch;
        }
        else
        {
            borrowed_batch.emplace(input, start, end, ownership_mode);
            input_batch = &borrowed_batch->batch;
        }

        auto status = makeStatus();
        auto output = makeBatch();
        api.drive_input_batch(instance, INPUT_ID_SCALAR, input_batch, &status, &output);
        ensureStatusOk("drive_input_batch", status);
        result.warning_count += status.warning_count;
        appendCastOutputRows(output, result.output);
    }

    auto status = makeStatus();
    api.drive_end_of_input(instance, INPUT_ID_SCALAR, &status);
    ensureStatusOk("drive_end_of_input", status);

    auto continued_output = makeBatch();
    status = makeStatus();
    api.continue_output(instance, &status, &continued_output);
    if (status.kind == STATUS_KIND_OK)
    {
        if (status.code != STATUS_CODE_NONE)
            throwStatusError("continue_output", status);
        if (continued_output.row_count != 0 || continued_output.column_count != 0)
        {
            throw std::runtime_error(
                "continue_output returned unexpected rows for scalar cast execution");
        }
    }
    else if (!(status.kind == STATUS_KIND_PROTOCOL_ERROR && status.code == STATUS_CODE_INSTANCE_FINISH_ONLY))
    {
        throwStatusError("continue_output", status);
    }

    status = makeStatus();
    api.finish(instance, &status);
    ensureStatusOk("finish", status);

    return result;
}

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
    uint32_t max_block_size,
    bool build_end_with_output,
    bool probe_end_with_output)
{
    TiforthExecutionBuildRequestV2 build_request{};
    build_request.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
    build_request.plan_kind = plan_kind;
    build_request.ambient_requirement_mask = ambient_requirement_mask;
    build_request.sql_mode = 0;
    build_request.session_charset = session_charset;
    build_request.default_collation = default_collation;
    build_request.decimal_precision_is_set = false;
    build_request.decimal_precision = 0;
    build_request.decimal_scale_is_set = false;
    build_request.decimal_scale = 0;
    build_request.max_block_size = max_block_size;

    ExecutionHandles handles(api);
    TiforthExecutionInstanceHandleV2 * instance = handles.buildAndOpen(build_request);

    JoinRunResult result;
    const size_t partition_count = std::max<size_t>(1, partitions);
    std::vector<Utf8Int64JoinBatchOwned> retained_batches;
    if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
    {
        const auto batches_for_rows = [&](size_t row_count) {
            if (row_count == 0)
                return size_t{0};
            const size_t chunk_size = std::max<size_t>(1, (row_count + partition_count - 1) / partition_count);
            return (row_count + chunk_size - 1) / chunk_size;
        };
        retained_batches.reserve(batches_for_rows(build_rows.size()) + batches_for_rows(probe_rows.size()));
    }

    auto drive_input_rows = [&](const std::vector<Utf8Int64Row> & rows, uint32_t input_id) {
        const size_t chunk_size = std::max<size_t>(1, (rows.size() + partition_count - 1) / partition_count);
        for (size_t start = 0; start < rows.size(); start += chunk_size)
        {
            const size_t end = std::min(rows.size(), start + chunk_size);
            const TiforthBatchViewV2 * input_batch = nullptr;
            std::optional<Utf8Int64JoinBatchOwned> borrowed_batch;
            if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
            {
                retained_batches.emplace_back(rows, start, end, ownership_mode);
                input_batch = &retained_batches.back().batch;
            }
            else
            {
                borrowed_batch.emplace(rows, start, end, ownership_mode);
                input_batch = &borrowed_batch->batch;
            }

            auto status = makeStatus();
            auto output = makeBatch();
            api.drive_input_batch(instance, input_id, input_batch, &status, &output);
            ensureStatusKindOk("drive_input_batch", status);
            result.warning_count += status.warning_count;
            appendJoinOutputRows(output, result.rows);
            drainJoinOutput(api, instance, status, result);
        }
    };

    drive_input_rows(build_rows, INPUT_ID_BUILD);

    auto status = makeStatus();
    if (build_end_with_output)
    {
        auto output = makeBatch();
        api.drive_end_of_input_with_output(instance, INPUT_ID_BUILD, &status, &output);
        ensureStatusKindOk("drive_end_of_input_with_output(build)", status);
        result.warning_count += status.warning_count;
        appendJoinOutputRows(output, result.rows);
        drainJoinOutput(api, instance, status, result);
    }
    else
    {
        api.drive_end_of_input(instance, INPUT_ID_BUILD, &status);
        ensureStatusKindOk("drive_end_of_input(build)", status);
        result.warning_count += status.warning_count;
        drainJoinOutput(api, instance, status, result);
    }

    drive_input_rows(probe_rows, INPUT_ID_PROBE);

    status = makeStatus();
    if (probe_end_with_output)
    {
        auto output = makeBatch();
        api.drive_end_of_input_with_output(instance, INPUT_ID_PROBE, &status, &output);
        ensureStatusKindOk("drive_end_of_input_with_output(probe)", status);
        result.warning_count += status.warning_count;
        appendJoinOutputRows(output, result.rows);
        drainJoinOutput(api, instance, status, result);
    }
    else
    {
        api.drive_end_of_input(instance, INPUT_ID_PROBE, &status);
        ensureStatusKindOk("drive_end_of_input(probe)", status);
        result.warning_count += status.warning_count;
        drainJoinOutput(api, instance, status, result);
    }

    status = makeStatus();
    api.finish(instance, &status);
    ensureStatusOk("finish", status);

    result.rows = canonicalizeJoinRows(std::move(result.rows));
    return result;
}

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
    uint32_t max_block_size,
    bool build_end_with_output,
    bool probe_end_with_output)
{
    TiforthExecutionBuildRequestV2 build_request{};
    build_request.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
    build_request.plan_kind = plan_kind;
    build_request.ambient_requirement_mask = ambient_requirement_mask;
    build_request.sql_mode = 0;
    build_request.session_charset = session_charset;
    build_request.default_collation = default_collation;
    build_request.decimal_precision_is_set = false;
    build_request.decimal_precision = 0;
    build_request.decimal_scale_is_set = false;
    build_request.decimal_scale = 0;
    build_request.max_block_size = max_block_size;

    ExecutionHandles handles(api);
    TiforthExecutionInstanceHandleV2 * instance = handles.buildAndOpen(build_request);

    JoinRunResult result;
    const size_t partition_count = std::max<size_t>(1, partitions);
    std::vector<Int64Int64JoinBatchOwned> retained_batches;
    if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
    {
        const auto batches_for_rows = [&](size_t row_count) {
            if (row_count == 0)
                return size_t{0};
            const size_t chunk_size = std::max<size_t>(1, (row_count + partition_count - 1) / partition_count);
            return (row_count + chunk_size - 1) / chunk_size;
        };
        retained_batches.reserve(batches_for_rows(build_rows.size()) + batches_for_rows(probe_rows.size()));
    }

    auto drive_input_rows = [&](const std::vector<Int64Int64Row> & rows, uint32_t input_id) {
        const size_t chunk_size = std::max<size_t>(1, (rows.size() + partition_count - 1) / partition_count);
        for (size_t start = 0; start < rows.size(); start += chunk_size)
        {
            const size_t end = std::min(rows.size(), start + chunk_size);
            const TiforthBatchViewV2 * input_batch = nullptr;
            std::optional<Int64Int64JoinBatchOwned> borrowed_batch;
            if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
            {
                retained_batches.emplace_back(rows, start, end, ownership_mode);
                input_batch = &retained_batches.back().batch;
            }
            else
            {
                borrowed_batch.emplace(rows, start, end, ownership_mode);
                input_batch = &borrowed_batch->batch;
            }

            auto status = makeStatus();
            auto output = makeBatch();
            api.drive_input_batch(instance, input_id, input_batch, &status, &output);
            ensureStatusKindOk("drive_input_batch", status);
            result.warning_count += status.warning_count;
            appendJoinOutputRows(output, result.rows);
            drainJoinOutput(api, instance, status, result);
        }
    };

    drive_input_rows(build_rows, INPUT_ID_BUILD);

    auto status = makeStatus();
    if (build_end_with_output)
    {
        auto output = makeBatch();
        api.drive_end_of_input_with_output(instance, INPUT_ID_BUILD, &status, &output);
        ensureStatusKindOk("drive_end_of_input_with_output(build)", status);
        result.warning_count += status.warning_count;
        appendJoinOutputRows(output, result.rows);
        drainJoinOutput(api, instance, status, result);
    }
    else
    {
        api.drive_end_of_input(instance, INPUT_ID_BUILD, &status);
        ensureStatusKindOk("drive_end_of_input(build)", status);
        result.warning_count += status.warning_count;
        drainJoinOutput(api, instance, status, result);
    }

    drive_input_rows(probe_rows, INPUT_ID_PROBE);

    status = makeStatus();
    if (probe_end_with_output)
    {
        auto output = makeBatch();
        api.drive_end_of_input_with_output(instance, INPUT_ID_PROBE, &status, &output);
        ensureStatusKindOk("drive_end_of_input_with_output(probe)", status);
        result.warning_count += status.warning_count;
        appendJoinOutputRows(output, result.rows);
        drainJoinOutput(api, instance, status, result);
    }
    else
    {
        api.drive_end_of_input(instance, INPUT_ID_PROBE, &status);
        ensureStatusKindOk("drive_end_of_input(probe)", status);
        result.warning_count += status.warning_count;
        drainJoinOutput(api, instance, status, result);
    }

    status = makeStatus();
    api.finish(instance, &status);
    ensureStatusOk("finish", status);

    result.rows = canonicalizeJoinRows(std::move(result.rows));
    return result;
}

std::vector<JoinOutputRow> canonicalizeJoinRows(std::vector<JoinOutputRow> rows)
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

} // namespace DB::Tiforth
