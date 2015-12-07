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

import java.util.Arrays;

import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.util.AggregateBuilder;
import org.junit.Test;

public class RandomAggregateFunctionTest {

    private static final AggregateFunction FUNCTION = FunctionType.RANDOM;

    @Test
    public void testGetName() throws Exception {
        assertEquals("RANDOM", FUNCTION.getFunctionName());
        assertEquals("RANDOM", FUNCTION.toString());
    }

    @Test
    public void testDataType() throws Exception {
        assertEquals(ColumnType.BIGINT, FUNCTION.getExpectedColumnType(ColumnType.BIGINT));
        assertEquals(ColumnType.STRING, FUNCTION.getExpectedColumnType(ColumnType.STRING));
    }

    @Test
    public void testBuildAggregate() throws Exception {
        final AggregateBuilder<?> aggregateBuilder = FUNCTION.createAggregateBuilder();
        aggregateBuilder.add("foo");
        aggregateBuilder.add("bar");
        aggregateBuilder.add("baz");
        final Object aggregate = aggregateBuilder.getAggregate();

        assertTrue(Arrays.asList("foo", "bar", "baz").contains(aggregate));
    }
}
