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
        @Test
        void testLocateEmptySubstring() {
            StringLiteral subStr = new StringLiteral("");
            StringLiteral mainStr = new StringLiteral("hello");
            {
                IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(subStr, mainStr);
                assertEquals(1, result.getValue());
            }
            {
                IntegerLiteral position = new IntegerLiteral(0);
                IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(subStr, mainStr, position);
                assertEquals(0, result.getValue());
            }
            {
                IntegerLiteral position = new IntegerLiteral(1);
                IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(subStr, mainStr, position);
                assertEquals(1, result.getValue());
            }
            {
                IntegerLiteral position = new IntegerLiteral(3);
                IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(subStr, mainStr, position);
                assertEquals(3, result.getValue());
            }
            {
                IntegerLiteral position = new IntegerLiteral(10);
                IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(subStr, mainStr, position);
                assertEquals(0, result.getValue());
            }
        }

        @Test
        void testLocateWithPosition() {
            // Test locate with position where substring is found
            StringLiteral subStr = new StringLiteral("l");
            StringLiteral mainStr = new StringLiteral("hello");
            {
                IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(subStr, mainStr);
                assertEquals(3, result.getValue());
            }
            {
                IntegerLiteral position = new IntegerLiteral(3);
                IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(subStr, mainStr, position);
                assertEquals(3, result.getValue());
            }
            {
                IntegerLiteral position = new IntegerLiteral(4);
                IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(subStr, mainStr, position);
                assertEquals(4, result.getValue());
            }
            {
                IntegerLiteral position = new IntegerLiteral(10);
                IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(subStr, mainStr, position);
                assertEquals(0, result.getValue());
            }
            {
                IntegerLiteral position = new IntegerLiteral(-1);
                IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(subStr, mainStr, position);
                assertEquals(0, result.getValue());
            }
            {
                IntegerLiteral position = new IntegerLiteral(0);
                IntegerLiteral result = (IntegerLiteral) StringArithmetic.locate(subStr, mainStr, position);
                assertEquals(0, result.getValue());
            }
        }
    }
}
