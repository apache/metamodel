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

import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.util.AggregateBuilder;

/**
 * Represents an aggregate function to use in a SelectItem.
 * 
 * @see SelectItem
 */
public class DefaultAggregateFunction<T> implements AggregateFunction {

    String functionType;

    public DefaultAggregateFunction(String _functionType) {
        functionType = _functionType;
    }

    public AggregateBuilder<T> build() {
        if (functionType.equals("COUNT")) {
            return new CountAggregateBuilder();
        } else if (functionType.equals("AVG")) {
            return new AverageAggregateBuilder();
        } else if (functionType.equals("SUM")) {
            return new SumAggregateBuilder();
        } else if (functionType.equals("MAX")) {
            return new MaxAggregateBuilder();
        } else if (functionType.equals("MIN")) {
            return new MinAggregateBuilder();
        } else {
            return null;
        }
    }

    public ColumnType getExpectedColumnType(ColumnType type) {
        if (functionType.equals("COUNT")) {
            return ColumnType.BIGINT;
        } else if (functionType.equals("SUM")) {
            return ColumnType.DOUBLE;
        } else {
            return type;
        }
    }

    public Object evaluate(Iterable<?> values) {
        AggregateBuilder<?> builder = build();
        for (Object object : values) {
            builder.add(object);
        }
        return builder.getAggregate();
    }

    /**
     * Executes the function
     * 
     * @param values
     *            the values to be evaluated. If a value is null it won't be
     *            evaluated
     * @return the result of the function execution. The return type class is
     *         dependent on the FunctionType and the values provided. COUNT
     *         yields a Long, AVG and SUM yields Double values and MAX and MIN
     *         yields the type of the provided values.
     */
    public Object evaluate(Object... values) {
        AggregateBuilder<?> builder = build();
        for (Object object : values) {
            builder.add(object);
        }
        return builder.getAggregate();
    }

    @Override
    public String toString() {
        return functionType;
    }
}