/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.query;

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
