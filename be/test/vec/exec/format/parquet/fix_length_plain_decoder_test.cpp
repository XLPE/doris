// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/exec/format/parquet/fix_length_plain_decoder.h"

#include <gtest/gtest.h>

#include "util/slice.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

class FixLengthPlainDecoderTest : public ::testing::Test {
protected:
    void SetUp() override {}

    std::unique_ptr<uint8_t[]> _data;
    Slice _data_slice;
    size_t _type_length;
};

// Test basic decoding functionality
TEST_F(FixLengthPlainDecoderTest, test_basic_decode) {
    // Prepare test data: create fixed-length integer values
    int32_t values[3] = {123, 456, 789};
    size_t data_size = sizeof(values);

    _data = std::make_unique<uint8_t[]>(data_size);
    memcpy(_data.get(), values, data_size);

    _data_slice = Slice(_data.get(), data_size);
    _type_length = sizeof(int32_t);

    FixLengthPlainDecoder decoder;
    decoder.set_type_length(_type_length);
    ASSERT_TRUE(decoder.set_data(&_data_slice).ok());

    MutableColumnPtr column = ColumnVector<int32_t>::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    // Create selection vector without filter
    size_t num_values = 3;
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), num_values);
    auto* result_column = assert_cast<ColumnVector<int32_t>*>(column.get());

    EXPECT_EQ(result_column->get_data()[0], 123);
    EXPECT_EQ(result_column->get_data()[1], 456);
    EXPECT_EQ(result_column->get_data()[2], 789);
}

// Test decoding with filter
TEST_F(FixLengthPlainDecoderTest, test_decode_with_filter) {
    // Prepare test data: create fixed-length integer values
    int32_t values[3] = {123, 456, 789};
    size_t data_size = sizeof(values);

    _data = std::make_unique<uint8_t[]>(data_size);
    memcpy(_data.get(), values, data_size);

    _data_slice = Slice(_data.get(), data_size);
    _type_length = sizeof(int32_t);

    FixLengthPlainDecoder decoder;
    decoder.set_type_length(_type_length);
    ASSERT_TRUE(decoder.set_data(&_data_slice).ok());

    MutableColumnPtr column = ColumnVector<int32_t>::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    // Create filter vector [1,0,1]
    size_t num_values = 3;
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data = {1, 0, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 2); // 2 values after filtering
    auto* result_column = assert_cast<ColumnVector<int32_t>*>(column.get());

    EXPECT_EQ(result_column->get_data()[0], 123);
    EXPECT_EQ(result_column->get_data()[1], 789);
}

// Test decoding with filter and null
TEST_F(FixLengthPlainDecoderTest, test_decode_with_filter_and_null) {
    // Prepare test data: create fixed-length integer values
    int32_t values[2] = {123, 789};
    size_t data_size = sizeof(values);

    _data = std::make_unique<uint8_t[]>(data_size);
    memcpy(_data.get(), values, data_size);

    _data_slice = Slice(_data.get(), data_size);
    _type_length = sizeof(int32_t);

    FixLengthPlainDecoder decoder;
    decoder.set_type_length(_type_length);
    ASSERT_TRUE(decoder.set_data(&_data_slice).ok());

    MutableColumnPtr column = ColumnVector<int32_t>::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    // Create filter vector [1,0,1] and null vector [0,1,0]
    size_t num_values = 3;
    std::vector<uint16_t> run_length_null_map = {1, 1, 1}; // data: [123, null, 789]
    std::vector<uint8_t> filter_data = {1, 0, 1};          // filtered_data: [123, 789]

    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(
            select_vector.init(run_length_null_map, num_values, &null_map, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 2); // 2 values after filtering
    auto* result_column = assert_cast<ColumnVector<int32_t>*>(column.get());

    // Expected values after filtering and null handling
    std::vector<std::optional<int32_t>> expected_values = {123, 789};
    for (size_t i = 0; i < expected_values.size(); ++i) {
        if (expected_values[i].has_value()) {
            EXPECT_EQ(result_column->get_data()[i], expected_values[i].value())
                    << "Mismatch at value " << i;
            EXPECT_FALSE(null_map[i]) << "Expected non-null at position " << i;
        } else {
            EXPECT_TRUE(null_map[i]) << "Expected null at position " << i;
        }
    }
}

// Test skipping values
TEST_F(FixLengthPlainDecoderTest, test_skip_value) {
    // Prepare test data: create fixed-length integer values
    int32_t values[3] = {123, 456, 789};
    size_t data_size = sizeof(values);

    _data = std::make_unique<uint8_t[]>(data_size);
    memcpy(_data.get(), values, data_size);

    _data_slice = Slice(_data.get(), data_size);
    _type_length = sizeof(int32_t);

    FixLengthPlainDecoder decoder;
    decoder.set_type_length(_type_length);
    ASSERT_TRUE(decoder.set_data(&_data_slice).ok());

    // Skip first 2 values
    ASSERT_TRUE(decoder.skip_values(2).ok());

    MutableColumnPtr column = ColumnVector<int32_t>::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    // Create selection vector
    size_t num_values = 1;                                    // Total 3 values, skip 2, remaining 1
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), num_values);
    auto* result_column = assert_cast<ColumnVector<int32_t>*>(column.get());

    EXPECT_EQ(result_column->get_data()[0], 789);
}

// Regression test for the FLBA / fixed-length null-slot bug:
// when a row is NULL, the decoder MUST zero the underlying bytes instead of
// leaving them uninitialized / stale. Otherwise the downstream decimal
// converter reads garbage and may overflow on a narrow target type
// (e.g. decimal(38,10) -> decimal(15,2)) and throw InternalError.
//
// The test mimics the real parquet reader: the same physical column buffer is
// reused across pages. Page 1 fills every slot with a distinct non-zero pattern
// (so any "stale" byte left at a null slot is detectable). After clear()+reuse,
// page 2 decodes [CONTENT, NULL, CONTENT]; the NULL slot must be all zeros,
// while non-null slots keep their (re-decoded) values.
TEST_F(FixLengthPlainDecoderTest, test_null_slot_zeroed_on_reuse) {
    _type_length = 16; // FLBA decimal(38,10) -> 16 bytes per value
    const size_t num_values = 3;

    // Build 3 non-null FLBA values, each 16 bytes of a distinct non-zero pattern.
    std::vector<uint8_t> payload(num_values * _type_length);
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<uint8_t>(i + 1); // 1..48, all non-zero
    }
    _data = std::make_unique<uint8_t[]>(payload.size());
    memcpy(_data.get(), payload.data(), payload.size());
    _data_slice = Slice(_data.get(), payload.size());

    MutableColumnPtr column = DataTypeUInt8().create_column();
    DataTypePtr data_type = std::make_shared<DataTypeUInt8>();

    // ---------- Page 1: all 3 rows CONTENT (fills the reused buffer) ----------
    {
        FixLengthPlainDecoder decoder;
        decoder.set_type_length(_type_length);
        ASSERT_TRUE(decoder.set_data(&_data_slice).ok());

        std::vector<uint16_t> rlnm(1, num_values); // all CONTENT
        std::vector<uint8_t> filter(num_values, 1);
        FilterMap filter_map;
        ASSERT_TRUE(filter_map.init(filter.data(), filter.size(), false).ok());
        ColumnSelectVector select_vector;
        ASSERT_TRUE(select_vector.init(rlnm, num_values, nullptr, &filter_map, 0).ok());

        ASSERT_TRUE(decoder.decode_values(column, data_type, select_vector, false).ok());
        ASSERT_EQ(column->size(), num_values * _type_length);
    }

    // ---------- Page 2: [CONTENT, NULL, CONTENT], reuse the same buffer ----------
    // clear() keeps capacity, so the bytes written in page 1 survive in the
    // (now logically empty) buffer. A NULL row must be zeroed by the decoder.
    column->clear();
    // Declared outside the block so it can be inspected after decoding.
    NullMap null_map;
    {
        FixLengthPlainDecoder decoder;
        decoder.set_type_length(_type_length);
        ASSERT_TRUE(decoder.set_data(&_data_slice).ok());

        std::vector<uint16_t> rlnm = {1, 1, 1}; // CONTENT, NULL_DATA, CONTENT
        std::vector<uint8_t> filter(num_values, 1);
        FilterMap filter_map;
        ASSERT_TRUE(filter_map.init(filter.data(), filter.size(), false).ok());
        ColumnSelectVector select_vector;
        ASSERT_TRUE(select_vector.init(rlnm, num_values, &null_map, &filter_map, 0).ok());

        ASSERT_TRUE(decoder.decode_values(column, data_type, select_vector, false).ok());
    }

    const auto raw = column->get_raw_data();
    const uint8_t* bytes = reinterpret_cast<const uint8_t*>(raw.data);
    ASSERT_EQ(raw.size, num_values * _type_length);

    // Null slot is logical row 1 -> byte range [16, 32) must be all zero.
    for (size_t i = _type_length; i < 2 * _type_length; ++i) {
        EXPECT_EQ(bytes[i], 0) << "null slot byte " << i << " should be zeroed";
    }
    // Non-null slots keep their (re-decoded) values. A NULL row consumes no
    // input bytes (the NULL_DATA branch does not advance _offset), so the second
    // CONTENT row reads payload[16..31], not payload[32..47].
    EXPECT_EQ(bytes[0], 1);   // row0 = payload[0..15]
    EXPECT_EQ(bytes[32], 17); // row2 = payload[16..31]
    // Sanity check that the middle row is really the null one.
    EXPECT_FALSE(null_map[0]);
    EXPECT_TRUE(null_map[1]);
    EXPECT_FALSE(null_map[2]);
}

} // namespace doris::vectorized
