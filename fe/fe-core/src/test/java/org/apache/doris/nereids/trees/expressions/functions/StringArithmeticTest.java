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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.trees.expressions.functions.executable.StringArithmetic;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class StringArithmeticTest {
    /**
     * locate function test units.
     */
    @Nested
    class LocateFunctionTest {

        private void assertLocate(String subStr, String mainStr, int expected) {
            IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(new StringLiteral(subStr),
                    new StringLiteral(mainStr));
            Assertions.assertEquals(expected, result.getValue(),
                    String.format("locate('%s', '%s') should return %d", subStr, mainStr, expected));
        }

        private void assertLocate(String subStr, String mainStr, int pos, int expected) {
            IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(new StringLiteral(subStr),
                    new StringLiteral(mainStr), new IntegerLiteral(pos));
            Assertions.assertEquals(expected, result.getValue(),
                    String.format("locate('%s', '%s', %d) should return %d", subStr, mainStr, pos, expected));
        }

        @Test
        void testLocateEmptySubstring() {
            assertLocate("", "hello", 1);
            assertLocate("", "hello", 0, 0);
            assertLocate("", "hello", 1, 1);
            assertLocate("", "hello", 3, 3);
            assertLocate("", "hello", 5, 5);
            assertLocate("", "hello", 10, 0);
        }

        @Test
        void testLocateWithPosition() {
            assertLocate("l", "hello", 3);
            assertLocate("l", "hello", 3, 3);
            assertLocate("l", "hello", 4, 4);
            assertLocate("l", "hello", 5, 0);
            assertLocate("l", "hello", 10, 0);
            assertLocate("l", "hello", -1, 0);
            assertLocate("l", "hello", 0, 0);
        }
    }
}
