package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.trees.expressions.functions.executable.StringArithmetic;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringArithmeticTest {
    /**
     * locate function test units.
     */
    @Nested
    class LocateFunctionTests {

        private void assertLocate(String subStr, String mainStr, int expected) {
            IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(new StringLiteral(subStr),
                    new StringLiteral(mainStr));
            assertEquals(expected, result.getValue(),
                    String.format("locate('%s', '%s') should return %d", subStr, mainStr, expected));
        }

        private void assertLocate(String subStr, String mainStr, int pos, int expected) {
            IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(new StringLiteral(subStr),
                    new StringLiteral(mainStr), new IntegerLiteral(pos));
            assertEquals(expected, result.getValue(),
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
