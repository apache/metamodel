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
 * Simple implementation of {@link NegationOperator}
 */
public class NegationOperatorImpl implements NegationOperator {

    private static final long serialVersionUID = 1L;

    private final String _sql;
    private final boolean _spaceDelimited;

    public NegationOperatorImpl(String sql, boolean spaceDelimited) {
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
        if (obj instanceof NegationOperator) {
            final NegationOperator other = (NegationOperator) obj;
            return isSpaceDelimited() == other.isSpaceDelimited() && Objects.equals(toSql(), other.toSql());
        }
        return false;
    }

/**
     * Converts from SQL string literals to an NegationOperator. Valid SQL values are "NOT" or NOTHING.
     *
     * @param sqlType
     * @return a NegationOperator object representing the specified SQL type
     */
    public static NegationOperator convertNegationOperator(String sqlType) {
        if (sqlType != null) {
            sqlType = sqlType.trim().toUpperCase();
            switch (sqlType) {
                case "NOT":
                    return NegationOperator.NOT;
                default:
                    return NegationOperator.NONE;
            }
        }
        return null;
    }
}
