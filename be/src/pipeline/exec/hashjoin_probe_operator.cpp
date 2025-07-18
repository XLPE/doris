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

#include "hashjoin_probe_operator.h"

#include <gen_cpp/PlanNodes_types.h>

#include <string>

#include "common/cast_set.h"
#include "common/logging.h"
#include "pipeline/exec/operator.h"
#include "runtime/descriptors.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
HashJoinProbeLocalState::HashJoinProbeLocalState(RuntimeState* state, OperatorXBase* parent)
        : JoinProbeLocalState<HashJoinSharedState, HashJoinProbeLocalState>(state, parent),
          _process_hashtable_ctx_variants(std::make_unique<HashTableCtxVariants>()) {}

Status HashJoinProbeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(JoinProbeLocalState::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _task_idx = info.task_idx;
    auto& p = _parent->cast<HashJoinProbeOperatorX>();
    _probe_expr_ctxs.resize(p._probe_expr_ctxs.size());
    for (size_t i = 0; i < _probe_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._probe_expr_ctxs[i]->clone(state, _probe_expr_ctxs[i]));
    }
    _other_join_conjuncts.resize(p._other_join_conjuncts.size());
    for (size_t i = 0; i < _other_join_conjuncts.size(); i++) {
        RETURN_IF_ERROR(p._other_join_conjuncts[i]->clone(state, _other_join_conjuncts[i]));
    }

    _mark_join_conjuncts.resize(p._mark_join_conjuncts.size());
    for (size_t i = 0; i < _mark_join_conjuncts.size(); i++) {
        RETURN_IF_ERROR(p._mark_join_conjuncts[i]->clone(state, _mark_join_conjuncts[i]));
    }

    _construct_mutable_join_block();
    _probe_column_disguise_null.reserve(_probe_expr_ctxs.size());
    _probe_arena_memory_usage = custom_profile()->AddHighWaterMarkCounter(
            "MemoryUsageProbeKeyArena", TUnit::BYTES, "", 1);
    // Probe phase
    _probe_expr_call_timer = ADD_TIMER(custom_profile(), "ProbeExprCallTime");
    _search_hashtable_timer = ADD_TIMER(custom_profile(), "ProbeWhenSearchHashTableTime");
    _build_side_output_timer = ADD_TIMER(custom_profile(), "ProbeWhenBuildSideOutputTime");
    _probe_side_output_timer = ADD_TIMER(custom_profile(), "ProbeWhenProbeSideOutputTime");
    _non_equal_join_conjuncts_timer =
            ADD_TIMER(custom_profile(), "NonEqualJoinConjunctEvaluationTime");
    _init_probe_side_timer = ADD_TIMER(custom_profile(), "InitProbeSideTime");
    return Status::OK();
}

Status HashJoinProbeLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(JoinProbeLocalState::open(state));

    auto& p = _parent->cast<HashJoinProbeOperatorX>();
    Status res;
    std::visit(
            [&](auto&& join_op_variants, auto have_other_join_conjunct) {
                using JoinOpType = std::decay_t<decltype(join_op_variants)>;
                if constexpr (JoinOpType::value == TJoinOp::CROSS_JOIN) {
                    res = Status::InternalError("hash join do not support cross join");
                } else {
                    _process_hashtable_ctx_variants
                            ->emplace<ProcessHashTableProbe<JoinOpType::value>>(
                                    this, state->batch_size());
                }
            },
            _shared_state->join_op_variants,
            vectorized::make_bool_variant(p._have_other_join_conjunct));
    return res;
}

void HashJoinProbeLocalState::prepare_for_next() {
    _probe_index = 0;
    _build_index = 0;
    _ready_probe = false;
    _last_probe_match = -1;
    _last_probe_null_mark = -1;
    _prepare_probe_block();
}

Status HashJoinProbeLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    if (_process_hashtable_ctx_variants) {
        std::visit(vectorized::Overload {[&](std::monostate&) {},
                                         [&](auto&& process_hashtable_ctx) {
                                             if (process_hashtable_ctx._arena) {
                                                 process_hashtable_ctx._arena.reset();
                                             }
                                         }},
                   *_process_hashtable_ctx_variants);
    }
    _process_hashtable_ctx_variants = nullptr;
    _null_map_column = nullptr;
    _probe_block.clear();
    return JoinProbeLocalState<HashJoinSharedState, HashJoinProbeLocalState>::close(state);
}

bool HashJoinProbeLocalState::_need_probe_null_map(vectorized::Block& block,
                                                   const std::vector<int>& res_col_ids) {
    for (size_t i = 0; i < _probe_expr_ctxs.size(); ++i) {
        const auto* column = block.get_by_position(res_col_ids[i]).column.get();
        if (column->is_nullable() &&
            !_parent->cast<HashJoinProbeOperatorX>()._serialize_null_into_key[i]) {
            return true;
        }
    }
    return false;
}

void HashJoinProbeLocalState::_prepare_probe_block() {
    // clear_column_data of _probe_block
    if (!_probe_column_disguise_null.empty()) {
        for (int i = 0; i < _probe_column_disguise_null.size(); ++i) {
            auto column_to_erase = _probe_column_disguise_null[i];
            _probe_block.erase(column_to_erase - i);
        }
        _probe_column_disguise_null.clear();
    }

    // remove add nullmap of probe columns
    for (auto index : _probe_column_convert_to_null) {
        auto& column_type = _probe_block.safe_get_by_position(index);
        DCHECK(column_type.column->is_nullable() || is_column_const(*(column_type.column.get())));
        DCHECK(column_type.type->is_nullable());

        column_type.column = remove_nullable(column_type.column);
        column_type.type = remove_nullable(column_type.type);
    }
    _key_columns_holder.clear();
    _probe_block.clear_column_data(_parent->get_child()->row_desc().num_materialized_slots());
}

HashJoinProbeOperatorX::HashJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                               int operator_id, const DescriptorTbl& descs)
        : JoinProbeOperatorX<HashJoinProbeLocalState>(pool, tnode, operator_id, descs),
          _join_distribution(tnode.hash_join_node.__isset.dist_type ? tnode.hash_join_node.dist_type
                                                                    : TJoinDistributionType::NONE),
          _is_broadcast_join(tnode.hash_join_node.__isset.is_broadcast_join &&
                             tnode.hash_join_node.is_broadcast_join),
          _hash_output_slot_ids(tnode.hash_join_node.__isset.hash_output_slot_ids
                                        ? tnode.hash_join_node.hash_output_slot_ids
                                        : std::vector<SlotId> {}),
          _partition_exprs(tnode.__isset.distribute_expr_lists && !_is_broadcast_join
                                   ? tnode.distribute_expr_lists[0]
                                   : std::vector<TExpr> {}) {}

Status HashJoinProbeOperatorX::pull(doris::RuntimeState* state, vectorized::Block* output_block,
                                    bool* eos) const {
    auto& local_state = get_local_state(state);
    if (local_state._shared_state->short_circuit_for_probe) {
        // If we use a short-circuit strategy, should return empty block directly.
        *eos = true;
        return Status::OK();
    }

    //TODO: this short circuit maybe could refactor, no need to check at here.
    if (local_state.empty_right_table_shortcut()) {
        // when build table rows is 0 and not have other_join_conjunct and join type is one of LEFT_OUTER_JOIN/FULL_OUTER_JOIN/LEFT_ANTI_JOIN
        // we could get the result is probe table + null-column(if need output)
        // If we use a short-circuit strategy, should return block directly by add additional null data.
        auto block_rows = local_state._probe_block.rows();
        if (local_state._probe_eos && block_rows == 0) {
            *eos = true;
            return Status::OK();
        }

        //create build side null column, if need output
        for (int i = 0;
             (_join_op != TJoinOp::LEFT_ANTI_JOIN) && i < _right_output_slot_flags.size(); ++i) {
            auto type = remove_nullable(_right_table_data_types[i]);
            auto column = type->create_column();
            column->resize(block_rows);
            auto null_map_column = vectorized::ColumnUInt8::create(block_rows, 1);
            auto nullable_column = vectorized::ColumnNullable::create(std::move(column),
                                                                      std::move(null_map_column));
            local_state._probe_block.insert({std::move(nullable_column), make_nullable(type),
                                             _right_table_column_names[i]});
        }

        /// No need to check the block size in `_filter_data_and_build_output` because here dose not
        /// increase the output rows count(just same as `_probe_block`'s rows count).
        RETURN_IF_ERROR(local_state.filter_data_and_build_output(state, output_block, eos,
                                                                 &local_state._probe_block, false));
        local_state._probe_block.clear_column_data(_child->row_desc().num_materialized_slots());
        return Status::OK();
    }

    local_state._join_block.clear_column_data();

    vectorized::MutableBlock mutable_join_block(&local_state._join_block);
    vectorized::Block temp_block;

    Status st;
    if (local_state._probe_index < local_state._probe_block.rows()) {
        DCHECK(local_state._has_set_need_null_map_for_probe);
        std::visit(
                [&](auto&& arg, auto&& process_hashtable_ctx) {
                    using HashTableProbeType = std::decay_t<decltype(process_hashtable_ctx)>;
                    if constexpr (!std::is_same_v<HashTableProbeType, std::monostate>) {
                        using HashTableCtxType = std::decay_t<decltype(arg)>;
                        if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                            st = process_hashtable_ctx.process(
                                    arg,
                                    local_state._null_map_column
                                            ? local_state._null_map_column->get_data().data()
                                            : nullptr,
                                    mutable_join_block, &temp_block,
                                    cast_set<uint32_t>(local_state._probe_block.rows()),
                                    _is_mark_join);
                        } else {
                            st = Status::InternalError("uninited hash table");
                        }
                    } else {
                        st = Status::InternalError("uninited hash table probe");
                    }
                },
                local_state._shared_state->hash_table_variant_vector.size() == 1
                        ? local_state._shared_state->hash_table_variant_vector[0]->method_variant
                        : local_state._shared_state
                                  ->hash_table_variant_vector[local_state._task_idx]
                                  ->method_variant,
                *local_state._process_hashtable_ctx_variants);
    } else if (local_state._probe_eos) {
        if (_is_right_semi_anti || (_is_outer_join && _join_op != TJoinOp::LEFT_OUTER_JOIN)) {
            std::visit(
                    [&](auto&& arg, auto&& process_hashtable_ctx) {
                        using HashTableProbeType = std::decay_t<decltype(process_hashtable_ctx)>;
                        if constexpr (!std::is_same_v<HashTableProbeType, std::monostate>) {
                            using HashTableCtxType = std::decay_t<decltype(arg)>;
                            if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                                st = process_hashtable_ctx.finish_probing(
                                        arg, mutable_join_block, &temp_block, eos, _is_mark_join);
                            } else {
                                st = Status::InternalError("uninited hash table");
                            }
                        } else {
                            st = Status::InternalError("uninited hash table probe");
                        }
                    },
                    local_state._shared_state->hash_table_variant_vector.size() == 1
                            ? local_state._shared_state->hash_table_variant_vector[0]
                                      ->method_variant
                            : local_state._shared_state
                                      ->hash_table_variant_vector[local_state._task_idx]
                                      ->method_variant,
                    *local_state._process_hashtable_ctx_variants);
        } else {
            *eos = true;
            return Status::OK();
        }
    } else {
        return Status::OK();
    }
    if (!st) {
        return st;
    }

    local_state._estimate_memory_usage += temp_block.allocated_bytes();
    RETURN_IF_ERROR(
            local_state.filter_data_and_build_output(state, output_block, eos, &temp_block));
    // Here make _join_block release the columns' ptr
    local_state._join_block.set_columns(local_state._join_block.clone_empty_columns());
    mutable_join_block.clear();
    return Status::OK();
}

std::string HashJoinProbeLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}, short_circuit_for_probe: {}",
                   JoinProbeLocalState<HashJoinSharedState, HashJoinProbeLocalState>::debug_string(
                           indentation_level),
                   _shared_state ? std::to_string(_shared_state->short_circuit_for_probe) : "NULL");
    return fmt::to_string(debug_string_buffer);
}

Status HashJoinProbeLocalState::_extract_join_column(vectorized::Block& block,
                                                     const std::vector<int>& res_col_ids) {
    if (empty_right_table_shortcut()) {
        return Status::OK();
    }

    _probe_columns.resize(_probe_expr_ctxs.size());

    if (!_has_set_need_null_map_for_probe) {
        _has_set_need_null_map_for_probe = true;
        _need_null_map_for_probe = _need_probe_null_map(block, res_col_ids);
    }
    if (_need_null_map_for_probe) {
        if (!_null_map_column) {
            _null_map_column = vectorized::ColumnUInt8::create();
        }
        _null_map_column->get_data().assign(block.rows(), (uint8_t)0);
    }

    auto& shared_state = *_shared_state;
    for (size_t i = 0; i < shared_state.build_exprs_size; ++i) {
        const auto* column = block.get_by_position(res_col_ids[i]).column.get();
        if (!column->is_nullable() &&
            _parent->cast<HashJoinProbeOperatorX>()._serialize_null_into_key[i]) {
            _key_columns_holder.emplace_back(
                    vectorized::make_nullable(block.get_by_position(res_col_ids[i]).column));
            _probe_columns[i] = _key_columns_holder.back().get();
        } else if (const auto* nullable = check_and_get_column<vectorized::ColumnNullable>(*column);
                   nullable &&
                   !_parent->cast<HashJoinProbeOperatorX>()._serialize_null_into_key[i]) {
            // update nulllmap and split nested out of ColumnNullable when serialize_null_into_key is false and column is nullable
            const auto& col_nested = nullable->get_nested_column();
            const auto& col_nullmap = nullable->get_null_map_data();
            DCHECK(_null_map_column);
            vectorized::VectorizedUtils::update_null_map(_null_map_column->get_data(), col_nullmap);
            _probe_columns[i] = &col_nested;
        } else {
            _probe_columns[i] = column;
        }
    }
    return Status::OK();
}

std::vector<uint16_t> HashJoinProbeLocalState::_convert_block_to_null(vectorized::Block& block) {
    std::vector<uint16_t> results;
    for (int i = 0; i < block.columns(); ++i) {
        if (auto& column_type = block.safe_get_by_position(i); !column_type.type->is_nullable()) {
            DCHECK(!column_type.column->is_nullable());
            column_type.column = make_nullable(column_type.column);
            column_type.type = make_nullable(column_type.type);
            results.emplace_back(i);
        }
    }
    return results;
}

Status HashJoinProbeLocalState::filter_data_and_build_output(RuntimeState* state,
                                                             vectorized::Block* output_block,
                                                             bool* eos,
                                                             vectorized::Block* temp_block,
                                                             bool check_rows_count) {
    auto output_rows = temp_block->rows();
    if (check_rows_count) {
        DCHECK(output_rows <= state->batch_size());
    }
    {
        SCOPED_TIMER(_join_filter_timer);
        RETURN_IF_ERROR(filter_block(_conjuncts, temp_block, temp_block->columns()));
    }

    RETURN_IF_ERROR(_build_output_block(temp_block, output_block));
    reached_limit(output_block, eos);
    return Status::OK();
}

bool HashJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = state->get_local_state(operator_id())->cast<HashJoinProbeLocalState>();
    return (local_state._probe_block.rows() == 0 ||
            local_state._probe_index == local_state._probe_block.rows()) &&
           !local_state._probe_eos && !local_state._shared_state->short_circuit_for_probe;
}

Status HashJoinProbeOperatorX::_do_evaluate(vectorized::Block& block,
                                            vectorized::VExprContextSPtrs& exprs,
                                            RuntimeProfile::Counter& expr_call_timer,
                                            std::vector<int>& res_col_ids) const {
    for (size_t i = 0; i < exprs.size(); ++i) {
        int result_col_id = -1;
        // execute build column
        {
            SCOPED_TIMER(&expr_call_timer);
            RETURN_IF_ERROR(exprs[i]->execute(&block, &result_col_id));
        }

        // TODO: opt the column is const
        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();
        res_col_ids[i] = result_col_id;
    }
    return Status::OK();
}

Status HashJoinProbeOperatorX::push(RuntimeState* state, vectorized::Block* input_block,
                                    bool eos) const {
    auto& local_state = get_local_state(state);
    local_state.prepare_for_next();
    local_state._probe_eos = eos;

    const auto rows = input_block->rows();
    size_t origin_size = input_block->allocated_bytes();

    if (rows > 0) {
        COUNTER_UPDATE(local_state._probe_rows_counter, rows);
        std::vector<int> res_col_ids(local_state._probe_expr_ctxs.size());
        RETURN_IF_ERROR(_do_evaluate(*input_block, local_state._probe_expr_ctxs,
                                     *local_state._probe_expr_call_timer, res_col_ids));
        if (_join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN) {
            local_state._probe_column_convert_to_null =
                    local_state._convert_block_to_null(*input_block);
        }

        RETURN_IF_ERROR(local_state._extract_join_column(*input_block, res_col_ids));

        local_state._estimate_memory_usage += (input_block->allocated_bytes() - origin_size);

        if (&local_state._probe_block != input_block) {
            input_block->swap(local_state._probe_block);
            COUNTER_SET(local_state._memory_used_counter,
                        (int64_t)local_state._probe_block.allocated_bytes());
        }
    }
    return Status::OK();
}

Status HashJoinProbeOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX<HashJoinProbeLocalState>::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        vectorized::VExprContextSPtr ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(eq_join_conjunct.left, ctx));
        _probe_expr_ctxs.push_back(ctx);

        /// null safe equal means null = null is true, the operator in SQL should be: <=>.
        const bool is_null_safe_equal =
                eq_join_conjunct.__isset.opcode &&
                (eq_join_conjunct.opcode == TExprOpcode::EQ_FOR_NULL) &&
                // For a null safe equal join, FE may generate a plan that
                // both sides of the conjuct are not nullable, we just treat it
                // as a normal equal join conjunct.
                (eq_join_conjunct.right.nodes[0].is_nullable ||
                 eq_join_conjunct.left.nodes[0].is_nullable);

        if (eq_join_conjuncts.size() == 1) {
            // single column key serialize method must use nullmap for represent null to instead serialize null into key
            _serialize_null_into_key.emplace_back(false);
        } else if (is_null_safe_equal) {
            // use serialize null into key to represent multi column null value
            _serialize_null_into_key.emplace_back(true);
        } else {
            // on normal conditions, because null!=null, it can be expressed directly with nullmap.
            _serialize_null_into_key.emplace_back(false);
        }
    }

    if (tnode.hash_join_node.__isset.other_join_conjuncts &&
        !tnode.hash_join_node.other_join_conjuncts.empty()) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                tnode.hash_join_node.other_join_conjuncts, _other_join_conjuncts));

        DCHECK(!_build_unique);
        DCHECK(_have_other_join_conjunct);
    } else if (tnode.hash_join_node.__isset.vother_join_conjunct) {
        _other_join_conjuncts.resize(1);
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(
                tnode.hash_join_node.vother_join_conjunct, _other_join_conjuncts[0]));

        // If LEFT SEMI JOIN/LEFT ANTI JOIN with not equal predicate,
        // build table should not be deduplicated.
        DCHECK(!_build_unique);
        DCHECK(_have_other_join_conjunct);
    }

    if (tnode.hash_join_node.__isset.mark_join_conjuncts &&
        !tnode.hash_join_node.mark_join_conjuncts.empty()) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                tnode.hash_join_node.mark_join_conjuncts, _mark_join_conjuncts));
        DCHECK(_is_mark_join);

        /// We make mark join conjuncts as equal conjuncts for null aware join,
        /// so `_mark_join_conjuncts` should be empty if this is null aware join.
        DCHECK_EQ(_mark_join_conjuncts.empty(),
                  _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                          _join_op == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN);
    }

    return Status::OK();
}

Status HashJoinProbeOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX<HashJoinProbeLocalState>::prepare(state));
    // init left/right output slots flags, only column of slot_id in _hash_output_slot_ids need
    // insert to output block of hash join.
    // _left_output_slots_flags : column of left table need to output set flag = true
    // _rgiht_output_slots_flags : column of right table need to output set flag = true
    // if _hash_output_slot_ids is empty, means all column of left/right table need to output.
    auto init_output_slots_flags = [&](auto& tuple_descs, auto& output_slot_flags,
                                       bool init_finalize_flag = false) {
        for (const auto& tuple_desc : tuple_descs) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                output_slot_flags.emplace_back(
                        std::find(_hash_output_slot_ids.begin(), _hash_output_slot_ids.end(),
                                  slot_desc->id()) != _hash_output_slot_ids.end());
                if (init_finalize_flag && output_slot_flags.back() &&
                    slot_desc->type()->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
                    _need_finalize_variant_column = true;
                }
            }
        }
    };
    init_output_slots_flags(_child->row_desc().tuple_descriptors(), _left_output_slot_flags, true);
    init_output_slots_flags(_build_side_child->row_desc().tuple_descriptors(),
                            _right_output_slot_flags);
    // _other_join_conjuncts are evaluated in the context of the rows produced by this node
    for (auto& conjunct : _other_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, *_intermediate_row_desc));
        conjunct->root()->collect_slot_column_ids(_should_not_lazy_materialized_column_ids);
    }

    for (auto& conjunct : _mark_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, *_intermediate_row_desc));
        conjunct->root()->collect_slot_column_ids(_should_not_lazy_materialized_column_ids);
    }

    RETURN_IF_ERROR(vectorized::VExpr::prepare(_probe_expr_ctxs, state, _child->row_desc()));
    DCHECK(_build_side_child != nullptr);
    // right table data types
    _right_table_data_types =
            vectorized::VectorizedUtils::get_data_types(_build_side_child->row_desc());
    _left_table_data_types = vectorized::VectorizedUtils::get_data_types(_child->row_desc());
    _right_table_column_names =
            vectorized::VectorizedUtils::get_column_names(_build_side_child->row_desc());

    std::vector<const SlotDescriptor*> slots_to_check;
    for (const auto& tuple_descriptor : _intermediate_row_desc->tuple_descriptors()) {
        for (const auto& slot : tuple_descriptor->slots()) {
            slots_to_check.emplace_back(slot);
        }
    }

    if (_is_mark_join) {
        const auto* last_one = slots_to_check.back();
        slots_to_check.pop_back();
        auto data_type = last_one->get_data_type_ptr();
        if (!data_type->is_nullable()) {
            return Status::InternalError(
                    "The last column for mark join should be Nullable(UInt8), not {}",
                    data_type->get_name());
        }

        const auto& null_data_type = assert_cast<const vectorized::DataTypeNullable&>(*data_type);
        if (null_data_type.get_nested_type()->get_primitive_type() != PrimitiveType::TYPE_BOOLEAN) {
            return Status::InternalError(
                    "The last column for mark join should be Nullable(UInt8), not {}",
                    data_type->get_name());
        }
    }

    _right_col_idx = (_is_right_semi_anti && !_have_other_join_conjunct &&
                      (!_is_mark_join || _mark_join_conjuncts.empty()))
                             ? 0
                             : _left_table_data_types.size();

    size_t idx = 0;
    for (const auto* slot : slots_to_check) {
        auto data_type = slot->get_data_type_ptr();
        const auto slot_on_left = idx < _right_col_idx;

        if (slot_on_left) {
            if (idx >= _left_table_data_types.size()) {
                return Status::InternalError(
                        "Join node(id={}, OP={}) intermediate slot({}, #{})'s on left table "
                        "idx out bound of _left_table_data_types: {} vs {}",
                        _node_id, _join_op, slot->col_name(), slot->id(), idx,
                        _left_table_data_types.size());
            }
        } else if (idx - _right_col_idx >= _right_table_data_types.size()) {
            return Status::InternalError(
                    "Join node(id={}, OP={}) intermediate slot({}, #{})'s on right table "
                    "idx out bound of _right_table_data_types: {} vs {}(idx = {}, _right_col_idx = "
                    "{})",
                    _node_id, _join_op, slot->col_name(), slot->id(), idx - _right_col_idx,
                    _right_table_data_types.size(), idx, _right_col_idx);
        }

        auto target_data_type = slot_on_left ? _left_table_data_types[idx]
                                             : _right_table_data_types[idx - _right_col_idx];
        ++idx;
        if (data_type->equals(*target_data_type)) {
            continue;
        }

        /// For outer join(left/right/full), the non-nullable columns may be converted to nullable.
        const auto accept_nullable_not_match =
                _join_op == TJoinOp::FULL_OUTER_JOIN ||
                (slot_on_left ? _join_op == TJoinOp::RIGHT_OUTER_JOIN
                              : _join_op == TJoinOp::LEFT_OUTER_JOIN);

        if (accept_nullable_not_match) {
            auto data_type_non_nullable = vectorized::remove_nullable(data_type);
            if (data_type_non_nullable->equals(*target_data_type)) {
                continue;
            }
        } else if (data_type->equals(*target_data_type)) {
            continue;
        }

        return Status::InternalError(
                "Join node(id={}, OP={}) intermediate slot({}, #{})'s on {} table data type not "
                "match: '{}' vs '{}'",
                _node_id, _join_op, slot->col_name(), slot->id(), (slot_on_left ? "left" : "right"),
                data_type->get_name(), target_data_type->get_name());
    }

    _build_side_child.reset();
    RETURN_IF_ERROR(vectorized::VExpr::open(_probe_expr_ctxs, state));
    for (auto& conjunct : _other_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->open(state));
    }

    for (auto& conjunct : _mark_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->open(state));
    }

    return Status::OK();
}

} // namespace doris::pipeline
