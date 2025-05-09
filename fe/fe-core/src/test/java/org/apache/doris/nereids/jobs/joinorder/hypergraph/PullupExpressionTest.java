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

package org.apache.doris.nereids.jobs.joinorder.hypergraph;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.exploration.mv.ComparisonResult;
import org.apache.doris.nereids.rules.exploration.mv.HyperGraphComparator;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PullupExpressionTest extends SqlTestBase {
    @Test
    void testPullUpQueryFilter() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1 join T2 on T1.id = T2.id where T1.id = 1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 join T2 on T1.id = T2.id",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertEquals(1, res.getQueryExpressions().size());
        Assertions.assertEquals("(id = 1)", res.getQueryExpressions().get(0).toSql());
    }

    @Test
    void testPullUpQueryJoinCondition() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1 join T2 on T1.id = T2.id and T1.score = T2.score",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 join T2 on T1.id = T2.id",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertEquals(1, res.getQueryExpressions().size());
        Assertions.assertEquals("(score = score)", res.getQueryExpressions().get(0).toSql());
    }

    @Test
    void testPullUpViewFilter() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1 join T2 on T1.id = T2.id",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 join T2 on T1.id = T2.id where T1.id = 2 and T2.id = 1",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertEquals(2, res.getViewExpressions().size());
        if (res.getViewExpressions().get(0).toSql().equals("(id = 2)")) {
            Assertions.assertEquals("(id = 1)", res.getViewExpressions().get(1).toSql());
        } else {
            Assertions.assertEquals("(id = 1)", res.getViewExpressions().get(0).toSql());
            Assertions.assertEquals("(id = 2)", res.getViewExpressions().get(1).toSql());
        }
    }

    @Test
    void testPullUpViewJoinCondition() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1 join T2 on T1.id = T2.id ",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 join T2 on T1.id = T2.id and T1.score = T2.score",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertEquals(1, res.getViewExpressions().size());
        Assertions.assertEquals("(score = score)", res.getViewExpressions().get(0).toSql());
    }
}
