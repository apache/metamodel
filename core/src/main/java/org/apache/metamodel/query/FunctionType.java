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

import java.io.Serializable;

import org.apache.metamodel.schema.ColumnType;

/**
 * Represents a generic function to use in a SelectItem.
 *
 * @see SelectItem
 */
public interface FunctionType extends Serializable {

    public static final AggregateFunction COUNT = new CountAggregateFunction();
    public static final AggregateFunction AVG = new AverageAggregateFunction();
    public static final AggregateFunction SUM = new SumAggregateFunction();
    public static final AggregateFunction MAX = new MaxAggregateFunction();
    public static final AggregateFunction MIN = new MinAggregateFunction();
    public static final AggregateFunction RANDOM = new RandomAggregateFunction();
    public static final AggregateFunction FIRST = new FirstAggregateFunction();
    public static final AggregateFunction LAST = new LastAggregateFunction();
    public static final ScalarFunction TO_STRING = new ToStringFunction();
    public static final ScalarFunction TO_NUMBER = new ToNumberFunction();
    public static final ScalarFunction TO_DATE = new ToDateFunction();
    public static final ScalarFunction TO_BOOLEAN = new ToBooleanFunction();
    public static final ScalarFunction MAP_VALUE = new MapValueFunction();

    public ColumnType getExpectedColumnType(ColumnType type);

    public String getFunctionName();
}
