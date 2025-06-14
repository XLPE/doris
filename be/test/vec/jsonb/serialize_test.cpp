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
#include "vec/jsonb/serialize.h"

#include <gen_cpp/Descriptors_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <math.h>
#include <stdint.h>

#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/exception.h"
#include "gen_cpp/descriptors.pb.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/hll.h"
#include "olap/olap_common.h"
#include "olap/tablet_schema.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/bitmap_value.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

void fill_block_with_array_int(vectorized::Block& block) {
    auto off_column = vectorized::ColumnOffset64::create();
    auto data_column = vectorized::ColumnInt32::create();
    // init column array with [[1,2,3],[],[4],[5,6]]
    std::vector<vectorized::ColumnArray::Offset64> offs = {0, 3, 3, 4, 6};
    std::vector<int32_t> vals = {1, 2, 3, 4, 5, 6};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }

    auto column_array_ptr =
            vectorized::ColumnArray::create(std::move(data_column), std::move(off_column));
    vectorized::DataTypePtr nested_type(std::make_shared<vectorized::DataTypeInt32>());
    vectorized::DataTypePtr array_type(std::make_shared<vectorized::DataTypeArray>(nested_type));
    vectorized::ColumnWithTypeAndName test_array_int(std::move(column_array_ptr), array_type,
                                                     "test_array_int");
    block.insert(test_array_int);
}

void fill_block_with_array_string(vectorized::Block& block) {
    auto off_column = vectorized::ColumnOffset64::create();
    auto data_column = vectorized::ColumnString::create();
    // init column array with [["abc","de"],["fg"],[], [""]];
    std::vector<vectorized::ColumnArray::Offset64> offs = {0, 2, 3, 3, 4};
    std::vector<std::string> vals = {"abc", "de", "fg", ""};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data(v.data(), v.size());
    }

    auto column_array_ptr =
            vectorized::ColumnArray::create(std::move(data_column), std::move(off_column));
    vectorized::DataTypePtr nested_type(std::make_shared<vectorized::DataTypeString>());
    vectorized::DataTypePtr array_type(std::make_shared<vectorized::DataTypeArray>(nested_type));
    vectorized::ColumnWithTypeAndName test_array_string(std::move(column_array_ptr), array_type,
                                                        "test_array_string");
    block.insert(test_array_string);
}

TEST(BlockSerializeTest, Array) {
    TabletSchema schema;
    TabletColumn c1;
    TabletColumn c2;
    c1.set_name("k1");
    c1.set_unique_id(1);
    c1.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    c2.set_name("k2");
    c2.set_unique_id(2);
    c2.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    schema.append_column(c1);
    schema.append_column(c2);
    // array int and array string
    vectorized::Block block;
    fill_block_with_array_int(block);
    fill_block_with_array_string(block);
    MutableColumnPtr col = ColumnString::create();
    // serialize
    JsonbSerializeUtil::block_to_jsonb(schema, block, static_cast<ColumnString&>(*col.get()),
                                       block.columns(),
                                       create_data_type_serdes(block.get_data_types()), {});
    // deserialize
    TupleDescriptor read_desc(PTupleDescriptor(), true);
    // slot1
    TSlotDescriptor tslot1;
    tslot1.__set_colName("k1");
    tslot1.nullIndicatorBit = -1;
    tslot1.nullIndicatorByte = 0;
    DataTypePtr type_desc = std::make_shared<vectorized::DataTypeArray>(
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_INT, true));
    tslot1.__set_slotType(type_desc->to_thrift());
    tslot1.__set_col_unique_id(1);
    SlotDescriptor* slot = new SlotDescriptor(tslot1);
    read_desc.add_slot(slot);

    // slot2
    TSlotDescriptor tslot2;
    tslot2.__set_colName("k2");
    tslot2.nullIndicatorBit = -1;
    tslot2.nullIndicatorByte = 0;
    DataTypePtr type_desc2 = std::make_shared<vectorized::DataTypeArray>(
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_STRING, true));
    tslot2.__set_slotType(type_desc2->to_thrift());
    tslot2.__set_col_unique_id(2);
    SlotDescriptor* slot2 = new SlotDescriptor(tslot2);
    read_desc.add_slot(slot2);

    Block new_block = block.clone_empty();
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    std::vector<std::string> default_values;
    default_values.resize(read_desc.slots().size());
    for (int i = 0; i < read_desc.slots().size(); ++i) {
        col_uid_to_idx[read_desc.slots()[i]->col_unique_id()] = i;
        default_values[i] = read_desc.slots()[i]->col_default_value();
        std::cout << "uid " << read_desc.slots()[i]->col_unique_id() << ":" << i << std::endl;
    }
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    THROW_IF_ERROR(JsonbSerializeUtil::jsonb_to_block(
            create_data_type_serdes(read_desc.slots()), static_cast<ColumnString&>(*col.get()),
            col_uid_to_idx, new_block, default_values, {}));
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    EXPECT_EQ(block.dump_data(), new_block.dump_data());
}

TEST(BlockSerializeTest, Map) {
    TabletSchema schema;
    TabletColumn map;
    map.set_name("m");
    map.set_unique_id(1);
    map.set_type(FieldType::OLAP_FIELD_TYPE_MAP);
    schema.append_column(map);
    // map string string
    DataTypePtr s = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr d = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr m = std::make_shared<DataTypeMap>(s, d);
    Array k1, k2, v1, v2;
    k1.push_back(Field::create_field<TYPE_STRING>("null"));
    k1.push_back(Field::create_field<TYPE_STRING>("doris"));
    k1.push_back(Field::create_field<TYPE_STRING>("clever amory"));
    v1.push_back(Field::create_field<TYPE_STRING>("ss"));
    v1.push_back(Field());
    v1.push_back(Field::create_field<TYPE_STRING>("NULL"));
    k2.push_back(Field::create_field<TYPE_STRING>("hello amory"));
    k2.push_back(Field::create_field<TYPE_STRING>("NULL"));
    k2.push_back(Field::create_field<TYPE_STRING>("cute amory"));
    k2.push_back(Field::create_field<TYPE_STRING>("doris"));
    v2.push_back(Field::create_field<TYPE_STRING>("s"));
    v2.push_back(Field::create_field<TYPE_STRING>("0"));
    v2.push_back(Field::create_field<TYPE_STRING>("sf"));
    v2.push_back(Field());
    Map m1, m2;
    m1.push_back(Field::create_field<TYPE_ARRAY>(k1));
    m1.push_back(Field::create_field<TYPE_ARRAY>(v1));
    m2.push_back(Field::create_field<TYPE_ARRAY>(k2));
    m2.push_back(Field::create_field<TYPE_ARRAY>(v2));
    MutableColumnPtr map_column = m->create_column();
    map_column->reserve(2);
    map_column->insert(Field::create_field<TYPE_MAP>(m1));
    map_column->insert(Field::create_field<TYPE_MAP>(m2));
    vectorized::ColumnWithTypeAndName type_and_name(map_column->get_ptr(), m, "test_map");
    vectorized::Block block;
    block.insert(type_and_name);

    MutableColumnPtr col = ColumnString::create();
    // serialize
    std::cout << "serialize to jsonb" << std::endl;
    JsonbSerializeUtil::block_to_jsonb(schema, block, static_cast<ColumnString&>(*col.get()),
                                       block.columns(),
                                       create_data_type_serdes(block.get_data_types()), {});
    // deserialize
    TupleDescriptor read_desc(PTupleDescriptor(), true);
    // slot
    TSlotDescriptor tslot;
    tslot.__set_colName("m");
    tslot.nullIndicatorBit = -1;
    tslot.nullIndicatorByte = 0;
    DataTypes sub_types;
    sub_types.reserve(2);
    sub_types.push_back(
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_STRING, true));
    sub_types.push_back(vectorized::DataTypeFactory::instance().create_data_type(TYPE_INT, true));
    DataTypePtr type_desc = std::make_shared<vectorized::DataTypeMap>(sub_types[0], sub_types[1]);
    tslot.__set_col_unique_id(1);
    tslot.__set_slotType(type_desc->to_thrift());
    SlotDescriptor* slot = new SlotDescriptor(tslot);
    read_desc.add_slot(slot);

    Block new_block = block.clone_empty();
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    std::vector<std::string> default_values;
    default_values.resize(read_desc.slots().size());
    for (int i = 0; i < read_desc.slots().size(); ++i) {
        col_uid_to_idx[read_desc.slots()[i]->col_unique_id()] = i;
        default_values[i] = read_desc.slots()[i]->col_default_value();
        std::cout << "uid " << read_desc.slots()[i]->col_unique_id() << ":" << i << std::endl;
    }
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    std::cout << "deserialize from jsonb" << std::endl;
    THROW_IF_ERROR(JsonbSerializeUtil::jsonb_to_block(
            create_data_type_serdes(read_desc.slots()), static_cast<ColumnString&>(*col.get()),
            col_uid_to_idx, new_block, default_values, {}));
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    EXPECT_EQ(block.dump_data(), new_block.dump_data());
}

TEST(BlockSerializeTest, Bigstr) {
    DataTypePtr s = std::make_shared<DataTypeString>();
    MutableColumnPtr col = ColumnString::create();
    std::string bigdata;
    bigdata.resize(std::numeric_limits<int32_t>::max() - 5);
    col->insert_data(bigdata.data(), bigdata.length());
    try {
        s->get_uncompressed_serialized_bytes(*col, BeExecVersionManager::get_newest_version());
    } catch (std::exception e) {
        return;
    }
    assert(false);
}

TEST(BlockSerializeTest, Struct) {
    TabletSchema schema;
    TabletColumn struct_col;
    struct_col.set_name("struct");
    struct_col.set_unique_id(1);
    struct_col.set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    schema.append_column(struct_col);
    vectorized::Block block;
    {
        DataTypePtr s = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr d = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
        DataTypePtr m = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
        DataTypePtr st = std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {s, d, m});
        Tuple t1, t2;
        t1.push_back(Field::create_field<TYPE_STRING>(String("amory cute")));
        t1.push_back(Field::create_field<TYPE_LARGEINT>(__int128_t(37)));
        t1.push_back(Field::create_field<TYPE_BOOLEAN>(true));
        t2.push_back(Field::create_field<TYPE_STRING>("null"));
        t2.push_back(Field::create_field<TYPE_LARGEINT>(__int128_t(26)));
        t2.push_back(Field::create_field<TYPE_BOOLEAN>(false));
        MutableColumnPtr struct_column = st->create_column();
        struct_column->reserve(2);
        struct_column->insert(Field::create_field<TYPE_STRUCT>(t1));
        struct_column->insert(Field::create_field<TYPE_STRUCT>(t2));
        vectorized::ColumnWithTypeAndName type_and_name(struct_column->get_ptr(), st,
                                                        "test_struct");
        block.insert(type_and_name);
    }

    MutableColumnPtr col = ColumnString::create();
    // serialize
    std::cout << "serialize to jsonb" << std::endl;
    JsonbSerializeUtil::block_to_jsonb(schema, block, static_cast<ColumnString&>(*col.get()),
                                       block.columns(),
                                       create_data_type_serdes(block.get_data_types()), {});
    // deserialize
    TupleDescriptor read_desc(PTupleDescriptor(), true);
    // slot
    TSlotDescriptor tslot;
    tslot.__set_colName("struct");
    tslot.nullIndicatorBit = -1;
    tslot.nullIndicatorByte = 0;
    DataTypes sub_types;
    Strings names;
    sub_types.reserve(3);
    names.reserve(3);
    sub_types.push_back(
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_STRING, true));
    names.push_back("name");
    sub_types.push_back(
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_LARGEINT, true));
    names.push_back("age");
    sub_types.push_back(
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_BOOLEAN, true));
    names.push_back("is");
    DataTypePtr type_desc = std::make_shared<DataTypeStruct>(sub_types, names);
    tslot.__set_col_unique_id(1);
    tslot.__set_slotType(type_desc->to_thrift());
    SlotDescriptor* slot = new SlotDescriptor(tslot);
    read_desc.add_slot(slot);

    Block new_block = block.clone_empty();
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    std::vector<std::string> default_values;
    default_values.resize(read_desc.slots().size());
    for (int i = 0; i < read_desc.slots().size(); ++i) {
        col_uid_to_idx[read_desc.slots()[i]->col_unique_id()] = i;
        default_values[i] = read_desc.slots()[i]->col_default_value();
        std::cout << "uid " << read_desc.slots()[i]->col_unique_id() << ":" << i << std::endl;
    }
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    std::cout << "deserialize from jsonb" << std::endl;
    THROW_IF_ERROR(JsonbSerializeUtil::jsonb_to_block(
            create_data_type_serdes(read_desc.slots()), static_cast<ColumnString&>(*col.get()),
            col_uid_to_idx, new_block, default_values, {}));
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    EXPECT_EQ(block.dump_data(), new_block.dump_data());
}

TEST(BlockSerializeTest, JsonbBlock) {
    vectorized::Block block;
    TabletSchema schema;
    std::vector<std::tuple<std::string, FieldType, int, PrimitiveType>> cols {
            {"k1", FieldType::OLAP_FIELD_TYPE_INT, 1, TYPE_INT},
            {"k2", FieldType::OLAP_FIELD_TYPE_STRING, 2, TYPE_STRING},
            {"k3", FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 3, TYPE_DECIMAL128I},
            {"v1", FieldType::OLAP_FIELD_TYPE_BITMAP, 7, TYPE_BITMAP},
            {"v2", FieldType::OLAP_FIELD_TYPE_HLL, 8, TYPE_HLL},
            {"k4", FieldType::OLAP_FIELD_TYPE_STRING, 4, TYPE_STRING},
            {"k5", FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 5, TYPE_DECIMAL128I},
            {"k6", FieldType::OLAP_FIELD_TYPE_INT, 6, TYPE_INT},
            {"k9", FieldType::OLAP_FIELD_TYPE_DATEV2, 9, TYPE_DATEV2}};
    for (auto t : cols) {
        TabletColumn c;
        c.set_name(std::get<0>(t));
        c.set_type(std::get<1>(t));
        c.set_unique_id(std::get<2>(t));
        schema.append_column(c);
    }
    // int
    {
        auto vec = vectorized::ColumnInt32::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "test_int");
        block.insert(type_and_name);
    }
    // string
    {
        auto strcol = vectorized::ColumnString::create();
        for (int i = 0; i < 1024; ++i) {
            std::string is = std::to_string(i);
            strcol->insert_data(is.c_str(), is.size());
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
        vectorized::ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type,
                                                        "test_string");
        block.insert(type_and_name);
    }
    // decimal
    {
        vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9, true));
        auto decimal_column = decimal_data_type->create_column();
        auto& data = ((vectorized::ColumnDecimal<vectorized::Decimal<vectorized::Int128>>*)
                              decimal_column.get())
                             ->get_data();
        for (int i = 0; i < 1024; ++i) {
            __int128_t value = __int128_t(i * pow(10, 9) + i * pow(10, 8));
            data.push_back(value);
        }
        vectorized::ColumnWithTypeAndName type_and_name(decimal_column->get_ptr(),
                                                        decimal_data_type, "test_decimal");
        block.insert(type_and_name);
    }
    // bitmap
    {
        vectorized::DataTypePtr bitmap_data_type(std::make_shared<vectorized::DataTypeBitMap>());
        auto bitmap_column = bitmap_data_type->create_column();
        std::vector<BitmapValue>& container =
                ((vectorized::ColumnBitmap*)bitmap_column.get())->get_data();
        for (int i = 0; i < 1024; ++i) {
            BitmapValue bv;
            for (int j = 0; j <= i; ++j) {
                bv.add(j);
            }
            container.push_back(bv);
        }
        vectorized::ColumnWithTypeAndName type_and_name(bitmap_column->get_ptr(), bitmap_data_type,
                                                        "test_bitmap");
        block.insert(type_and_name);
    }
    // hll
    {
        vectorized::DataTypePtr hll_data_type(std::make_shared<vectorized::DataTypeHLL>());
        auto hll_column = hll_data_type->create_column();
        std::vector<HyperLogLog>& container =
                ((vectorized::ColumnHLL*)hll_column.get())->get_data();
        for (int i = 0; i < 1024; ++i) {
            HyperLogLog hll;
            hll.update(i);
            container.push_back(hll);
        }
        vectorized::ColumnWithTypeAndName type_and_name(hll_column->get_ptr(), hll_data_type,
                                                        "test_hll");

        block.insert(type_and_name);
    }
    // nullable string
    {
        vectorized::DataTypePtr string_data_type(std::make_shared<vectorized::DataTypeString>());
        vectorized::DataTypePtr nullable_data_type(
                std::make_shared<vectorized::DataTypeNullable>(string_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*)nullable_column.get())->insert_many_defaults(1024);
        vectorized::ColumnWithTypeAndName type_and_name(nullable_column->get_ptr(),
                                                        nullable_data_type, "test_nullable");
        block.insert(type_and_name);
    }
    // nullable decimal
    {
        vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9, true));
        vectorized::DataTypePtr nullable_data_type(
                std::make_shared<vectorized::DataTypeNullable>(decimal_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*)nullable_column.get())->insert_many_defaults(1024);
        vectorized::ColumnWithTypeAndName type_and_name(
                nullable_column->get_ptr(), nullable_data_type, "test_nullable_decimal");
        block.insert(type_and_name);
    }
    // int with 1024 batch size
    {
        auto column_vector_int32 = vectorized::ColumnInt32::create();
        auto column_nullable_vector = vectorized::make_nullable(std::move(column_vector_int32));
        auto mutable_nullable_vector = std::move(*column_nullable_vector).mutate();
        for (int i = 0; i < 1024; i++) {
            mutable_nullable_vector->insert(
                    Field::create_field<TYPE_INT>(vectorized::cast_to_nearest_field_type(i)));
        }
        auto data_type = vectorized::make_nullable(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::ColumnWithTypeAndName type_and_name(mutable_nullable_vector->get_ptr(),
                                                        data_type, "test_nullable_int32");
        block.insert(type_and_name);
    }
    // fill with datev2
    {
        auto column_vector_date_v2 = vectorized::ColumnDateV2::create();
        auto& date_v2_data = column_vector_date_v2->get_data();
        for (int i = 0; i < 1024; ++i) {
            DateV2Value<DateV2ValueType> value;
            value.unchecked_set_time(2022, 6, 6, 0, 0, 0, 0);
            date_v2_data.push_back(*reinterpret_cast<vectorized::UInt32*>(&value));
        }
        vectorized::DataTypePtr date_v2_type(std::make_shared<vectorized::DataTypeDateV2>());
        vectorized::ColumnWithTypeAndName test_date_v2(column_vector_date_v2->get_ptr(),
                                                       date_v2_type, "test_datev2");
        block.insert(test_date_v2);
    }
    MutableColumnPtr col = ColumnString::create();
    // serialize
    JsonbSerializeUtil::block_to_jsonb(schema, block, static_cast<ColumnString&>(*col.get()),
                                       block.columns(),
                                       create_data_type_serdes(block.get_data_types()), {});
    // deserialize
    TupleDescriptor read_desc(PTupleDescriptor(), true);
    for (auto t : cols) {
        TSlotDescriptor tslot;
        tslot.__set_colName(std::get<0>(t));
        if (std::get<3>(t) == TYPE_DECIMAL128I) {
            auto type_desc = vectorized::DataTypeFactory::instance().create_data_type(
                    std::get<3>(t), false, 27, 9);
            tslot.__set_slotType(type_desc->to_thrift());
        } else {
            auto type_desc =
                    vectorized::DataTypeFactory::instance().create_data_type(std::get<3>(t), false);
            tslot.__set_slotType(type_desc->to_thrift());
        }
        tslot.__set_col_unique_id(std::get<2>(t));
        SlotDescriptor* slot = new SlotDescriptor(tslot);
        read_desc.add_slot(slot);
    }
    Block new_block = block.clone_empty();
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    std::vector<std::string> default_values;
    default_values.resize(read_desc.slots().size());
    for (int i = 0; i < read_desc.slots().size(); ++i) {
        col_uid_to_idx[read_desc.slots()[i]->col_unique_id()] = i;
    }
    THROW_IF_ERROR(
            JsonbSerializeUtil::jsonb_to_block(create_data_type_serdes(block.get_data_types()),
                                               static_cast<const ColumnString&>(*col.get()),
                                               col_uid_to_idx, new_block, default_values, {}));
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    EXPECT_EQ(block.dump_data(), new_block.dump_data());
}
} // namespace doris::vectorized
