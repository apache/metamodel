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
package org.apache.metamodel.schema.typing;

import org.apache.metamodel.schema.ColumnType;

import java.util.List;

/**
 * Constructors and common utilities for {@link ColumnTypingStrategy} objects.
 */
public class ColumnTypingStrategies {

    private static final DefaultColumnTypingStrategy DEFAULT_STRATEGY = new DefaultColumnTypingStrategy();


    private ColumnTypingStrategies() {
    }


    /**
     * The default strategy assumes all column types are text.
     *
     * @return a strategy that defaults to text
     */
    public static ColumnTypingStrategy getDefaultStrategy() {
        return DEFAULT_STRATEGY;
    }


    /**
     * Utility to create a {@link CustomColumnTypingStrategy}.
     *
     * @param columnTypes the types of each column
     * @return a strategy for custom type configuration
     */
    public static ColumnTypingStrategy getCustomStrategy(final List<ColumnType> columnTypes) {
        return new CustomColumnTypingStrategy(columnTypes);
    }


    /**
     * Utility to create a {@link CustomColumnTypingStrategy}.
     *
     * @param columnTypes the types of each column
     * @return a strategy for custom type configuration
     */
    public static ColumnTypingStrategy getCustomStrategy(final ColumnType... columnTypes) {
        return new CustomColumnTypingStrategy(columnTypes);
    }
}
