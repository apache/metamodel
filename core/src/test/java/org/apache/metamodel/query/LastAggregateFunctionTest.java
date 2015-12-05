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

import static org.junit.Assert.*;

import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.util.AggregateBuilder;
import org.junit.Test;

public class LastAggregateFunctionTest {

    private static final AggregateFunction FUNCTION = FunctionType.LAST;

    @Test
    public void testGetName() throws Exception {
        assertEquals("LAST", FUNCTION.getFunctionName());
        assertEquals("LAST", FUNCTION.toString());
    }

    @Test
    public void testDataType() throws Exception {
        assertEquals(ColumnType.BIGINT, FUNCTION.getExpectedColumnType(ColumnType.BIGINT));
        assertEquals(ColumnType.STRING, FUNCTION.getExpectedColumnType(ColumnType.STRING));
    }

    @Test
    public void testBuildAggregate() throws Exception {
        final AggregateBuilder<?> aggregateBuilder = FUNCTION.createAggregateBuilder();
        aggregateBuilder.add("1");
        aggregateBuilder.add("2");
        aggregateBuilder.add("3");
        assertEquals("3", aggregateBuilder.getAggregate());
    }
}
