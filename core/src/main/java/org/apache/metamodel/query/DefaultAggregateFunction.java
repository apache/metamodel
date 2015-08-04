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
 * Implementation of the {@link org.apache.metamodel.query.AggregateFunction}.
 */
public abstract class DefaultAggregateFunction<T>  {

    public abstract AggregateBuilder<T> build();

    public abstract ColumnType getExpectedColumnType(ColumnType type);

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

    public abstract String getFunctionType();

    @Override
    public String toString() { return getFunctionType(); }

}