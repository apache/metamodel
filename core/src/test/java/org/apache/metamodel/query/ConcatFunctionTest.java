/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.query;

import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConcatFunctionTest {

    private final ScalarFunction function = FunctionType.CONCAT;

    @Test
    public void testConcatValues() throws Exception {
        MutableColumn column1 = new MutableColumn("column1", ColumnType.VARCHAR);
        MutableColumn column2 = new MutableColumn("column2", ColumnType.INTEGER);
        final SelectItem operandItem1 = new SelectItem(column1);
        final SelectItem operandItem2 = new SelectItem(column2);
        final Row row = new DefaultRow(new SimpleDataSetHeader(new SelectItem[] { operandItem1 , operandItem2}),
                new Object[] { 1 , "hello"});
        final Object v1 = function.evaluate(
                row,
                new Object[] { "column1", "\' stringtobeappended \'", "column2" },
                operandItem1);
        assertEquals("1 stringtobeappended hello", v1.toString());
    }

    @Test
    public void testNotAValidColumn() throws Exception {
        MutableColumn column1 = new MutableColumn("column1", ColumnType.VARCHAR);
        final SelectItem operandItem1 = new SelectItem(column1);
        final Row row = new DefaultRow(new SimpleDataSetHeader(new SelectItem[] { operandItem1 }),
                new Object[] { "hello" });
        final Object v1 = function.evaluate(
                row,
                new Object[] { "column1", "\' stringtobeappended \'", "column2" },
                operandItem1);
        assertEquals("hello stringtobeappended null", v1.toString());
    }

}