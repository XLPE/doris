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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.parser.Location;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Slot has not been bound.
 */
public class UnboundSlot extends Slot implements Unbound, PropagateNullable {

    private Optional<Location> slotLocation;
    private final List<String> nameParts;

    public UnboundSlot(String... nameParts) {
        this(null, nameParts);
    }

    public UnboundSlot(@Nullable Location location, String... nameParts) {
        this(location, ImmutableList.copyOf(nameParts), Optional.empty());
    }

    public UnboundSlot(List<String> nameParts) {
        this(null, nameParts, Optional.empty());
    }

    public UnboundSlot(@Nullable Location location, List<String> nameParts) {
        this(location, ImmutableList.copyOf(nameParts), Optional.empty());
    }

    public UnboundSlot(
            @Nullable Location location, List<String> nameParts, Optional<Pair<Integer, Integer>> indexInSqlString) {
        super(indexInSqlString);
        this.nameParts = ImmutableList.copyOf(Objects.requireNonNull(nameParts, "nameParts can not be null"));
        this.slotLocation = Optional.ofNullable(location);
    }

    @Override
    public Slot withIndexInSql(Pair<Integer, Integer> index) {
        return new UnboundSlot(slotLocation.orElse(null), nameParts, Optional.ofNullable(index));
    }

    public List<String> getNameParts() {
        return nameParts;
    }

    @Override
    public String getName() {
        return nameParts.stream().map(n -> {
            if (n.contains(".")) {
                return "`" + n + "`";
            } else {
                return n;
            }
        }).reduce((left, right) -> left + "." + right).orElse("");
    }

    @Override
    public List<String> getQualifier() {
        return nameParts.subList(0, nameParts.size() - 1);
    }

    @Override
    public String computeToSql() {
        switch (nameParts.size()) {
            case 1: return Utils.quoteIfNeeded(nameParts.get(0));
            case 2: return Utils.quoteIfNeeded(nameParts.get(0)) + "." + Utils.quoteIfNeeded(nameParts.get(1));
            case 3: return Utils.quoteIfNeeded(nameParts.get(0)) + "." + Utils.quoteIfNeeded(nameParts.get(1))
                    + "." + Utils.quoteIfNeeded(nameParts.get(2));
            default: {
                return nameParts.stream().map(Utils::quoteIfNeeded)
                        .reduce((left, right) -> left + "." + right)
                        .orElse("");
            }
        }
    }

    public static UnboundSlot quoted(String name) {
        return new UnboundSlot(null, Lists.newArrayList(name), Optional.empty());
    }

    @Override
    public String toString() {
        return "'" + getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnboundSlot other = (UnboundSlot) o;
        return nameParts.equals(other.nameParts);
    }

    @Override
    public int computeHashCode() {
        return nameParts.hashCode();
    }

    public Optional<Location> getLocation() {
        return slotLocation;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundSlot(this, context);
    }
}
