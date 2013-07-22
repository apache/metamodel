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

/**
 * Defines the types of operators that can be used in filters.
 * 
 * @see FilterItem
 */
public enum OperatorType {

    EQUALS_TO("="), DIFFERENT_FROM("<>"), LIKE("LIKE"), GREATER_THAN(">"), LESS_THAN("<"), IN("IN"),

    /**
     * @deprecated use {@link #LESS_THAN} instead.
     */
    @Deprecated
    LOWER_THAN("<"),

    /**
     * @deprecated use {@link #GREATER_THAN} instead.
     */
    @Deprecated
    HIGHER_THAN(">");

    private final String _sql;

    private OperatorType(String sql) {
        _sql = sql;
    }

    public String toSql() {
        return _sql;
    }

    /**
     * Converts from SQL string literals to an OperatorType. Valid SQL values
     * are "=", "<>", "LIKE", ">" and "<".
     * 
     * @param sqlType
     * @return a OperatorType object representing the specified SQL type
     */
    public static OperatorType convertOperatorType(String sqlType) {
        if (sqlType != null) {
            for (OperatorType operator : values()) {
                if (sqlType.equals(operator.toSql())) {
                    return operator;
                }
            }
        }
        return null;
    }
}
