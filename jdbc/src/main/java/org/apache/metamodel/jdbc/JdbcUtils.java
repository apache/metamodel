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
package org.apache.metamodel.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.jdbc.dialects.DefaultQueryRewriter;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.QueryParameter;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.FormatHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various internal utility methods for the JDBC module of MetaModel.
 */
public final class JdbcUtils {

    private static final Logger logger = LoggerFactory.getLogger(JdbcUtils.class);

    public static MetaModelException wrapException(SQLException e, String actionDescription) throws MetaModelException {
        String message = e.getMessage();
        if (message == null || message.isEmpty()) {
            message = "Could not " + actionDescription;
        } else {
            message = "Could not " + actionDescription + ": " + message;
        }

        logger.error(message, e);
        logger.error("Error code={}, SQL state={}", e.getErrorCode(), e.getSQLState());

        final SQLException nextException = e.getNextException();
        if (nextException != null) {
            logger.error("Next SQL exception: " + nextException.getMessage(), nextException);
        }

        return new MetaModelException(message, e);
    }

    /**
     * @deprecated use {@link IQueryRewriter#setStatementParameter(PreparedStatement, int, Column, Object)}
     */
    @Deprecated
    public static void setStatementValue(final PreparedStatement st, final int valueIndex, final Column column,
            Object value) throws SQLException {
        new DefaultQueryRewriter(null).setStatementParameter(st, valueIndex, column, value);
    }

    public static String getValueAsSql(Column column, Object value, IQueryRewriter queryRewriter) {
        if (value == null) {
            return "NULL";
        }
        final ColumnType columnType = column.getType();
        if (columnType.isLiteral() && value instanceof String) {
            value = queryRewriter.escapeQuotes((String) value);
        }
        final String formatSqlValue = FormatHelper.formatSqlValue(columnType, value);
        return formatSqlValue;
    }

    public static String createWhereClause(List<FilterItem> whereItems, IQueryRewriter queryRewriter,
            boolean inlineValues) {
        if (whereItems.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(" WHERE ");
        boolean firstValue = true;
        for (FilterItem whereItem : whereItems) {
            if (firstValue) {
                firstValue = false;
            } else {
                sb.append(" AND ");
            }
            if (!inlineValues) {
                if (isPreparedParameterCandidate(whereItem)) {
                    // replace operator with parameter
                    whereItem = new FilterItem(whereItem.getSelectItem(), whereItem.getOperator(),
                            new QueryParameter());
                }
            }
            final String whereItemLabel = queryRewriter.rewriteFilterItem(whereItem);
            sb.append(whereItemLabel);
        }
        return sb.toString();
    }

    /**
     * Determines if a particular {@link FilterItem} will have it's parameter
     * (operand) replaced during SQL generation. Such filter items should
     * succesively have their parameters set at execution time.
     * 
     * @param whereItem
     * @return
     */
    public static boolean isPreparedParameterCandidate(FilterItem whereItem) {
        return !whereItem.isCompoundFilter() && !OperatorType.IN.equals(whereItem.getOperator())
                && whereItem.getOperand() != null;
    }

    public static String[] getTableTypesAsStrings(TableType[] tableTypes) {
        String[] types = new String[tableTypes.length];
        for (int i = 0; i < types.length; i++) {
            if (tableTypes[i] == TableType.OTHER) {
                // if the OTHER type has been selected, don't use a table
                // pattern (ie. include all types)
                types = null;
                break;
            }
            types[i] = tableTypes[i].toString();
        }
        return types;
    }
}
