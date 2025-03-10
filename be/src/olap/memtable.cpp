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

#include "olap/memtable.h"

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <pdqsort.h>

#include <algorithm>
#include <limits>
#include <string>
#include <vector>

#include "bvar/bvar.h"
#include "common/config.h"
#include "olap/memtable_memory_limiter.h"
#include "olap/olap_define.h"
#include "olap/tablet_schema.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "tablet_meta.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column.h"

namespace doris {

bvar::Adder<int64_t> g_memtable_cnt("memtable_cnt");
bvar::Adder<int64_t> g_memtable_input_block_allocated_size("memtable_input_block_allocated_size");

using namespace ErrorCode;

MemTable::MemTable(int64_t tablet_id, std::shared_ptr<TabletSchema> tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   bool enable_unique_key_mow, PartialUpdateInfo* partial_update_info)
        : _mem_type(MemType::ACTIVE),
          _tablet_id(tablet_id),
          _enable_unique_key_mow(enable_unique_key_mow),
          _keys_type(tablet_schema->keys_type()),
          _tablet_schema(tablet_schema),
          _is_first_insertion(true),
          _agg_functions(tablet_schema->num_columns()),
          _offsets_of_aggregate_states(tablet_schema->num_columns()),
          _total_size_of_aggregate_states(0) {
    g_memtable_cnt << 1;
    _query_thread_context.init_unlocked();
    _arena = std::make_unique<vectorized::Arena>();
    _vec_row_comparator = std::make_shared<RowInBlockComparator>(_tablet_schema);
    _num_columns = _tablet_schema->num_columns();
    if (partial_update_info != nullptr) {
        _is_partial_update = partial_update_info->is_partial_update;
        if (_is_partial_update) {
            _num_columns = partial_update_info->partial_update_input_columns.size();
            if (partial_update_info->is_schema_contains_auto_inc_column &&
                !partial_update_info->is_input_columns_contains_auto_inc_column) {
                _is_partial_update_and_auto_inc = true;
                _num_columns += 1;
            }
        }
    }
    // TODO: Support ZOrderComparator in the future
    _init_columns_offset_by_slot_descs(slot_descs, tuple_desc);
    _mem_tracker = std::make_shared<MemTracker>();
}

void MemTable::_init_columns_offset_by_slot_descs(const std::vector<SlotDescriptor*>* slot_descs,
                                                  const TupleDescriptor* tuple_desc) {
    for (auto slot_desc : *slot_descs) {
        const auto& slots = tuple_desc->slots();
        for (int j = 0; j < slots.size(); ++j) {
            if (slot_desc->id() == slots[j]->id()) {
                _column_offset.emplace_back(j);
                break;
            }
        }
    }
    if (_is_partial_update_and_auto_inc) {
        _column_offset.emplace_back(_column_offset.size());
    }
}

void MemTable::_init_agg_functions(const vectorized::Block* block) {
    for (uint32_t cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
        vectorized::AggregateFunctionPtr function;
        if (_keys_type == KeysType::UNIQUE_KEYS && _enable_unique_key_mow) {
            // In such table, non-key column's aggregation type is NONE, so we need to construct
            // the aggregate function manually.
            function = vectorized::AggregateFunctionSimpleFactory::instance().get(
                    "replace_load", {block->get_data_type(cid)},
                    block->get_data_type(cid)->is_nullable(),
                    BeExecVersionManager::get_newest_version());
        } else {
            function = _tablet_schema->column(cid).get_aggregate_function(
                    vectorized::AGG_LOAD_SUFFIX, _tablet_schema->column(cid).get_be_exec_version());
            if (function == nullptr) {
                LOG(WARNING) << "column get aggregate function failed, column="
                             << _tablet_schema->column(cid).name();
            }
        }

        DCHECK(function != nullptr);
        _agg_functions[cid] = function;
    }

    for (uint32_t cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
        _offsets_of_aggregate_states[cid] = _total_size_of_aggregate_states;
        _total_size_of_aggregate_states += _agg_functions[cid]->size_of_data();

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (cid + 1 < _num_columns) {
            size_t alignment_of_next_state = _agg_functions[cid + 1]->align_of_data();

            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            _total_size_of_aggregate_states =
                    (_total_size_of_aggregate_states + alignment_of_next_state - 1) /
                    alignment_of_next_state * alignment_of_next_state;
        }
    }
}

MemTable::~MemTable() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_query_thread_context.query_mem_tracker);
    if (_is_flush_success) {
        // If the memtable is flush success, then its memtracker's consumption should be 0
        if (_mem_tracker->consumption() != 0 && config::crash_in_memory_tracker_inaccurate) {
            LOG(FATAL) << "memtable flush success but cosumption is not 0, it is "
                       << _mem_tracker->consumption();
        }
    }
    g_memtable_input_block_allocated_size << -_input_mutable_block.allocated_bytes();
    g_memtable_cnt << -1;
    if (_keys_type != KeysType::DUP_KEYS) {
        for (auto it = _row_in_blocks.begin(); it != _row_in_blocks.end(); it++) {
            if (!(*it)->has_init_agg()) {
                continue;
            }
            // We should release agg_places here, because they are not released when a
            // load is canceled.
            for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
                auto function = _agg_functions[i];
                DCHECK(function != nullptr);
                function->destroy((*it)->agg_places(i));
            }
        }
    }
    std::for_each(_row_in_blocks.begin(), _row_in_blocks.end(), std::default_delete<RowInBlock>());
    _arena.reset();
    _vec_row_comparator.reset();
    _row_in_blocks.clear();
    _agg_functions.clear();
    _input_mutable_block.clear();
    _output_mutable_block.clear();
}

int RowInBlockComparator::operator()(const RowInBlock* left, const RowInBlock* right) const {
    return _pblock->compare_at(left->_row_pos, right->_row_pos, _tablet_schema->num_key_columns(),
                               *_pblock, -1);
}

Status MemTable::insert(const vectorized::Block* input_block,
                        const std::vector<uint32_t>& row_idxs) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    if (_is_first_insertion) {
        _is_first_insertion = false;
        auto clone_block = input_block->clone_without_columns(&_column_offset);
        _input_mutable_block = vectorized::MutableBlock::build_mutable_block(&clone_block);
        _vec_row_comparator->set_block(&_input_mutable_block);
        _output_mutable_block = vectorized::MutableBlock::build_mutable_block(&clone_block);
        if (_keys_type != KeysType::DUP_KEYS) {
            // there may be additional intermediate columns in input_block
            // we only need columns indicated by column offset in the output
            RETURN_IF_CATCH_EXCEPTION(_init_agg_functions(&clone_block));
        }
        if (_tablet_schema->has_sequence_col()) {
            if (_is_partial_update) {
                // for unique key partial update, sequence column index in block
                // may be different with the index in `_tablet_schema`
                for (size_t i = 0; i < clone_block.columns(); i++) {
                    if (clone_block.get_by_position(i).name == SEQUENCE_COL) {
                        _seq_col_idx_in_block = i;
                        break;
                    }
                }
            } else {
                _seq_col_idx_in_block = _tablet_schema->sequence_col_idx();
            }
        }
    }

    auto num_rows = row_idxs.size();
    size_t cursor_in_mutableblock = _input_mutable_block.rows();
    auto block_size0 = _input_mutable_block.allocated_bytes();
    RETURN_IF_ERROR(_input_mutable_block.add_rows(input_block, row_idxs.data(),
                                                  row_idxs.data() + num_rows, &_column_offset));
    auto block_size1 = _input_mutable_block.allocated_bytes();
    g_memtable_input_block_allocated_size << block_size1 - block_size0;
    for (int i = 0; i < num_rows; i++) {
        _row_in_blocks.emplace_back(new RowInBlock {cursor_in_mutableblock + i});
    }

    _stat.raw_rows += num_rows;
    return Status::OK();
}

void MemTable::_aggregate_two_row_in_block(vectorized::MutableBlock& mutable_block,
                                           RowInBlock* src_row, RowInBlock* dst_row) {
    if (_tablet_schema->has_sequence_col() && _seq_col_idx_in_block >= 0) {
        DCHECK_LT(_seq_col_idx_in_block, mutable_block.columns());
        auto col_ptr = mutable_block.mutable_columns()[_seq_col_idx_in_block].get();
        auto res = col_ptr->compare_at(dst_row->_row_pos, src_row->_row_pos, *col_ptr, -1);
        // dst sequence column larger than src, don't need to update
        if (res > 0) {
            return;
        }
        // need to update the row pos in dst row to the src row pos when has
        // sequence column
        dst_row->_row_pos = src_row->_row_pos;
    }
    // dst is non-sequence row, or dst sequence is smaller
    for (uint32_t cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
        auto col_ptr = mutable_block.mutable_columns()[cid].get();
        _agg_functions[cid]->add(dst_row->agg_places(cid),
                                 const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                                 src_row->_row_pos, _arena.get());
    }
}
Status MemTable::_put_into_output(vectorized::Block& in_block) {
    SCOPED_RAW_TIMER(&_stat.put_into_output_ns);
    std::vector<uint32_t> row_pos_vec;
    DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
    row_pos_vec.reserve(in_block.rows());
    for (int i = 0; i < _row_in_blocks.size(); i++) {
        row_pos_vec.emplace_back(_row_in_blocks[i]->_row_pos);
    }
    return _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                          row_pos_vec.data() + in_block.rows());
}

/**
 * @brief 对 MemTable 中的行进行排序，并合并新旧行。
 *
 * 该方法首先对新插入的行进行排序，然后将新行与旧行合并，同时计算具有相同键的行的数量。
 *
 * @return 具有相同键的行的数量。
 */
size_t MemTable::_sort() {
    // 记录排序操作的耗时
    SCOPED_RAW_TIMER(&_stat.sort_ns);
    // 增加排序次数统计
    _stat.sort_times++;
    // 初始化具有相同键的行的数量
    size_t same_keys_num = 0;
    // 创建一个 Tie 对象，用于指定需要排序的新行的范围
    Tie tie = Tie(_last_sorted_pos, _row_in_blocks.size());
    // 遍历所有键列
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
        // 定义一个比较函数，用于比较两行在指定键列上的值
        auto cmp = [&](const RowInBlock* lhs, const RowInBlock* rhs) -> int {
            return _input_mutable_block.compare_one_column(lhs->_row_pos, rhs->_row_pos, i, -1);
        };
        // 对指定范围内的行按当前键列进行排序
        _sort_one_column(_row_in_blocks, tie, cmp);
    }
    // 判断键类型是否为 DUP_KEYS
    bool is_dup = (_keys_type == KeysType::DUP_KEYS);
    // 对排序后的行进行额外的排序，以确保排序的稳定性
    auto iter = tie.iter();
    while (iter.next()) {
        // 使用 pdqsort 对指定范围内的行进行排序
        pdqsort(std::next(_row_in_blocks.begin(), iter.left()),
                std::next(_row_in_blocks.begin(), iter.right()),
                [&is_dup](const RowInBlock* lhs, const RowInBlock* rhs) -> bool {
                    // 根据键类型决定排序顺序
                    return is_dup ? lhs->_row_pos > rhs->_row_pos : lhs->_row_pos < rhs->_row_pos;
                });
        // 累加具有相同键的行的数量
        same_keys_num += iter.right() - iter.left();
    }
    // 设置行比较器的块为 _input_mutable_block
    _vec_row_comparator->set_block(&_input_mutable_block);
    // 定义一个合并比较函数，用于合并新旧行
    auto cmp_func = [this, is_dup, &same_keys_num](const RowInBlock* l,
                                                   const RowInBlock* r) -> bool {
        // 比较两行的键值
        auto value = (*(this->_vec_row_comparator))(l, r);
        if (value == 0) {
            // 如果键值相同，累加相同键的行的数量
            same_keys_num++;
            // 根据键类型决定排序顺序
            return is_dup ? l->_row_pos > r->_row_pos : l->_row_pos < r->_row_pos;
        } else {
            // 如果键值不同，按比较结果排序
            return value < 0;
        }
    };
    // 获取新行的起始迭代器
    auto new_row_it = std::next(_row_in_blocks.begin(), _last_sorted_pos);
    // 使用 std::inplace_merge 合并新旧行
    std::inplace_merge(_row_in_blocks.begin(), new_row_it, _row_in_blocks.end(), cmp_func);
    // 更新最后排序位置为当前行的数量
    _last_sorted_pos = _row_in_blocks.size();
    // 返回具有相同键的行的数量
    return same_keys_num;
}


Status MemTable::_sort_by_cluster_keys() {
    SCOPED_RAW_TIMER(&_stat.sort_ns);
    _stat.sort_times++;
    // sort all rows
    vectorized::Block in_block = _output_mutable_block.to_block();
    vectorized::MutableBlock mutable_block =
            vectorized::MutableBlock::build_mutable_block(&in_block);
    auto clone_block = in_block.clone_without_columns();
    _output_mutable_block = vectorized::MutableBlock::build_mutable_block(&clone_block);

    std::vector<RowInBlock*> row_in_blocks;
    std::unique_ptr<int, std::function<void(int*)>> row_in_blocks_deleter((int*)0x01, [&](int*) {
        std::for_each(row_in_blocks.begin(), row_in_blocks.end(),
                      std::default_delete<RowInBlock>());
    });
    row_in_blocks.reserve(mutable_block.rows());
    for (size_t i = 0; i < mutable_block.rows(); i++) {
        row_in_blocks.emplace_back(new RowInBlock {i});
    }
    Tie tie = Tie(0, mutable_block.rows());

    for (auto cid : _tablet_schema->cluster_key_idxes()) {
        auto index = _tablet_schema->field_index(cid);
        if (index == -1) {
            return Status::InternalError("could not find cluster key column with unique_id=" +
                                         std::to_string(cid) + " in tablet schema");
        }
        auto cmp = [&](const RowInBlock* lhs, const RowInBlock* rhs) -> int {
            return mutable_block.compare_one_column(lhs->_row_pos, rhs->_row_pos, index, -1);
        };
        _sort_one_column(row_in_blocks, tie, cmp);
    }

    // sort extra round by _row_pos to make the sort stable
    auto iter = tie.iter();
    while (iter.next()) {
        pdqsort(std::next(row_in_blocks.begin(), iter.left()),
                std::next(row_in_blocks.begin(), iter.right()),
                [](const RowInBlock* lhs, const RowInBlock* rhs) -> bool {
                    return lhs->_row_pos < rhs->_row_pos;
                });
    }

    in_block = mutable_block.to_block();
    SCOPED_RAW_TIMER(&_stat.put_into_output_ns);
    std::vector<uint32_t> row_pos_vec;
    DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
    row_pos_vec.reserve(in_block.rows());
    for (int i = 0; i < row_in_blocks.size(); i++) {
        row_pos_vec.emplace_back(row_in_blocks[i]->_row_pos);
    }
    return _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                          row_pos_vec.data() + in_block.rows(), &_column_offset);
}

void MemTable::_sort_one_column(std::vector<RowInBlock*>& row_in_blocks, Tie& tie,
                                std::function<int(const RowInBlock*, const RowInBlock*)> cmp) {
    auto iter = tie.iter();
    while (iter.next()) {
        pdqsort(std::next(row_in_blocks.begin(), iter.left()),
                std::next(row_in_blocks.begin(), iter.right()),
                [&cmp](auto lhs, auto rhs) -> bool { return cmp(lhs, rhs) < 0; });
        tie[iter.left()] = 0;
        for (int i = iter.left() + 1; i < iter.right(); i++) {
            tie[i] = (cmp(row_in_blocks[i - 1], row_in_blocks[i]) == 0);
        }
    }
}

template <bool is_final>
void MemTable::_finalize_one_row(RowInBlock* row,
                                 const vectorized::ColumnsWithTypeAndName& block_data,
                                 int row_pos) {
    // move key columns
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); ++i) {
        _output_mutable_block.get_column_by_position(i)->insert_from(*block_data[i].column.get(),
                                                                     row->_row_pos);
    }
    if (row->has_init_agg()) {
        // get value columns from agg_places
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            auto function = _agg_functions[i];
            auto* agg_place = row->agg_places(i);
            auto* col_ptr = _output_mutable_block.get_column_by_position(i).get();
            function->insert_result_into(agg_place, *col_ptr);

            if constexpr (is_final) {
                function->destroy(agg_place);
            } else {
                function->reset(agg_place);
            }
        }

        _arena->clear();

        if constexpr (is_final) {
            row->remove_init_agg();
        } else {
            for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
                auto function = _agg_functions[i];
                auto* agg_place = row->agg_places(i);
                auto* col_ptr = _output_mutable_block.get_column_by_position(i).get();
                function->add(agg_place, const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                              row_pos, _arena.get());
            }
        }
    } else {
        // move columns for rows do not need agg
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            _output_mutable_block.get_column_by_position(i)->insert_from(
                    *block_data[i].column.get(), row->_row_pos);
        }
    }
    if constexpr (!is_final) {
        row->_row_pos = row_pos;
    }
}

/**
 * @brief 对 MemTable 中的数据进行聚合操作
 *
 * 该方法会对 MemTable 中的行进行排序，并根据行的键值进行聚合操作。
 * 聚合操作会更新相同键值行的聚合状态，并将结果存储在输出块中。
 *
 * @tparam is_final 是否为最终聚合阶段
 */
template <bool is_final>
void MemTable::_aggregate() {
    // 记录聚合操作的耗时
    SCOPED_RAW_TIMER(&_stat.agg_ns);
    // 增加聚合次数统计
    _stat.agg_times++;
    // 将输入可变块转换为不可变块
    vectorized::Block in_block = _input_mutable_block.to_block();
    // 从不可变块创建一个新的可变块
    vectorized::MutableBlock mutable_block =
            vectorized::MutableBlock::build_mutable_block(&in_block);
    // 设置行比较器使用的块为新的可变块
    _vec_row_comparator->set_block(&mutable_block);
    // 获取不可变块中列的类型和名称信息
    auto& block_data = in_block.get_columns_with_type_and_name();
    // 临时存储行对象的向量
    std::vector<RowInBlock*> temp_row_in_blocks;
    // 预先分配足够的空间以存储行对象
    temp_row_in_blocks.reserve(_last_sorted_pos);
    // 指向先前处理的行对象的指针
    RowInBlock* prev_row = nullptr;
    // 当前行的位置
    int row_pos = -1;
    // 仅在需要时初始化聚合状态
    for (int i = 0; i < _row_in_blocks.size(); i++) {
        // 如果临时行向量不为空，并且当前行与前一行的键值相同
        if (!temp_row_in_blocks.empty() &&
            (*_vec_row_comparator)(prev_row, _row_in_blocks[i]) == 0) {
            // 如果前一行还没有初始化聚合状态
            if (!prev_row->has_init_agg()) {
                // 为前一行分配聚合状态所需的内存
                prev_row->init_agg_places(
                        _arena->aligned_alloc(_total_size_of_aggregate_states, 16),
                        _offsets_of_aggregate_states.data());
                // 遍历所有非键列
                for (auto cid = _tablet_schema->num_key_columns(); cid < _num_columns; cid++) {
                    // 获取当前列的指针
                    auto col_ptr = mutable_block.mutable_columns()[cid].get();
                    // 获取前一行的聚合状态存储位置
                    auto data = prev_row->agg_places(cid);
                    // 创建聚合状态
                    _agg_functions[cid]->create(data);
                    // 将前一行的数据添加到聚合状态中
                    _agg_functions[cid]->add(
                            data, const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                            prev_row->_row_pos, _arena.get());
                }
            }
            // 增加合并行的统计
            _stat.merged_rows++;
            // 聚合当前行和前一行的数据
            _aggregate_two_row_in_block(mutable_block, _row_in_blocks[i], prev_row);
        } else {
            // 更新前一行指针为当前行
            prev_row = _row_in_blocks[i];
            // 如果临时行向量不为空
            if (!temp_row_in_blocks.empty()) {
                // 对前一行进行最终处理
                _finalize_one_row<is_final>(temp_row_in_blocks.back(), block_data, row_pos);
            }
            // 将当前行添加到临时行向量中
            temp_row_in_blocks.push_back(prev_row);
            // 增加行位置
            row_pos++;
        }
    }
    // 如果临时行向量不为空
    if (!temp_row_in_blocks.empty()) {
        // 对最后一行进行最终处理
        _finalize_one_row<is_final>(temp_row_in_blocks.back(), block_data, row_pos);
    }
    // 如果不是最终聚合阶段
    if constexpr (!is_final) {
        // 将输出可变块的数据交换到输入可变块
        _input_mutable_block.swap(_output_mutable_block);
        // 创建一个空的不可变块
        std::unique_ptr<vectorized::Block> empty_input_block = in_block.create_same_struct_block(0);
        // 从空的不可变块创建一个新的可变块
        _output_mutable_block =
                vectorized::MutableBlock::build_mutable_block(empty_input_block.get());
        // 清空输出可变块的列数据
        _output_mutable_block.clear_column_data();
        // 更新行对象向量为临时行向量
        _row_in_blocks = temp_row_in_blocks;
        // 更新最后排序位置为当前行的数量
        _last_sorted_pos = _row_in_blocks.size();
    }
}


void MemTable::shrink_memtable_by_agg() {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    if (_keys_type == KeysType::DUP_KEYS) {
        return;
    }
    size_t same_keys_num = _sort();
    if (same_keys_num != 0) {
        _aggregate<false>();
    }
}

bool MemTable::need_flush() const {
    auto max_size = config::write_buffer_size;
    if (_is_partial_update) {
        auto update_columns_size = _num_columns;
        max_size = max_size * update_columns_size / _tablet_schema->num_columns();
        max_size = max_size > 1048576 ? max_size : 1048576;
    }
    return memory_usage() >= max_size;
}

bool MemTable::need_agg() const {
    if (_keys_type == KeysType::AGG_KEYS) {
        auto max_size = config::write_buffer_size_for_agg;
        return memory_usage() >= max_size;
    }
    return false;
}

Status MemTable::_to_block(std::unique_ptr<vectorized::Block>* res) {
    size_t same_keys_num = _sort();
    if (_keys_type == KeysType::DUP_KEYS || same_keys_num == 0) {
        if (_keys_type == KeysType::DUP_KEYS && _tablet_schema->num_key_columns() == 0) {
            _output_mutable_block.swap(_input_mutable_block);
        } else {
            vectorized::Block in_block = _input_mutable_block.to_block();
            RETURN_IF_ERROR(_put_into_output(in_block));
        }
    } else {
        _aggregate<true>();
    }
    if (_keys_type == KeysType::UNIQUE_KEYS && _enable_unique_key_mow &&
        !_tablet_schema->cluster_key_idxes().empty()) {
        if (_is_partial_update) {
            return Status::InternalError(
                    "Partial update for mow with cluster keys is not supported");
        }
        RETURN_IF_ERROR(_sort_by_cluster_keys());
    }
    g_memtable_input_block_allocated_size << -_input_mutable_block.allocated_bytes();
    _input_mutable_block.clear();
    // After to block, all data in arena is saved in the block
    _arena.reset();
    *res = vectorized::Block::create_unique(_output_mutable_block.to_block());
    return Status::OK();
}

Status MemTable::to_block(std::unique_ptr<vectorized::Block>* res) {
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_to_block(res));
    return Status::OK();
}

} // namespace doris
