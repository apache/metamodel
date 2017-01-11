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

import java.util.Objects;

/**
 * Simple implementation of {@link OperatorType}
 */
public class OperatorTypeImpl implements OperatorType {
    
    private static final long serialVersionUID = 1L;

    private final String _sql;
    private final boolean _spaceDelimited;

    public OperatorTypeImpl(String sql, boolean spaceDelimited) {
        _sql = sql;
        _spaceDelimited = spaceDelimited;
    }

    @Override
    public boolean isSpaceDelimited() {
        return _spaceDelimited;
    }

    @Override
    public String toSql() {
        return _sql;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (obj instanceof OperatorType) {
            // we only require another OperatorType, not necesarily an _Impl_.
            // This is to allow other implementations that wrap this. For
            // instance the implementation provided by
            // LegacyDeserializationObjectInputStream.
            final OperatorType other = (OperatorType) obj;
            return isSpaceDelimited() == other.isSpaceDelimited() && Objects.equals(toSql(), other.toSql());
        }
        return false;
    }

/**
     * Converts from SQL string literals to an OperatorType. Valid SQL values are "=", "&lt;&gt;", "LIKE", "&gt;", "&gt;=", "&lt;" and
     * "&lt;=".
     *
     * @param sqlType
     * @return a OperatorType object representing the specified SQL type
     */
    public static OperatorType convertOperatorType(String sqlType) {
        if (sqlType != null) {
            sqlType = sqlType.trim().toUpperCase();
            switch (sqlType) {
                case "=":
                case "==":
                case "EQ":
                case "EQUALS_TO":
                    return OperatorType.EQUALS_TO;
                case "<>":
                case "!=":
                case "NE":
                case "NOT_EQUAL":
                case "NOT_EQUAL_TO":
                case "NOT_EQUALS":
                case "NOT_EQUALS_TO":
                case "DIFFERENT_FROM":
                    return OperatorType.DIFFERENT_FROM;
                case ">":
                case "GT":
                case "GREATER_THAN":
                    return OperatorType.GREATER_THAN;
                case ">=":
                case "=>":
                case "GREATER_THAN_OR_EQUAL":
                    return OperatorType.GREATER_THAN_OR_EQUAL;
                case "NOT_IN":
                case "NOT IN":
                    return OperatorType.NOT_IN;
                case "IN":
                    return OperatorType.IN;
                case "<":
                case "LT":
                case "LESS_THAN":
                    return OperatorType.LESS_THAN;
                case "<=":
                case "=<":
                case "LESS_THAN_OR_EQUAL":
                    return OperatorType.LESS_THAN_OR_EQUAL;
                case "LIKE":
                    return OperatorType.LIKE;
                case "NOT_LIKE":
                case "NOT LIKE":
                    return OperatorType.NOT_LIKE;
            }
        }
        return null;
    }
}
