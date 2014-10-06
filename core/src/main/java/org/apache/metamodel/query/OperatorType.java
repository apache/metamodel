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

    EQUALS_TO("=", false),

    DIFFERENT_FROM("<>", false),

    LIKE("LIKE", true),

    GREATER_THAN(">", false),

    GREATER_THAN_OR_EQUAL(">=", false),

    LESS_THAN("<", false),

    LESS_THAN_OR_EQUAL("<=", false),

    IN("IN", true);

    private final String _sql;
    private final boolean _spaceDelimited;

    private OperatorType(String sql, boolean spaceDelimited) {
        _sql = sql;
        _spaceDelimited = spaceDelimited;
    }

/**
     * Determines if this operator requires a space delimitor. Operators that are written using letters usually require
     * space delimitation whereas sign-based operators such as "=" and "<" can be applied even without any delimitaton.
     */
    public boolean isSpaceDelimited() {
        return _spaceDelimited;
    }

    public String toSql() {
        return _sql;
    }

/**
     * Converts from SQL string literals to an OperatorType. Valid SQL values are "=", "<>", "LIKE", ">", ">=", "<" and
     * "<=".
     *
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
