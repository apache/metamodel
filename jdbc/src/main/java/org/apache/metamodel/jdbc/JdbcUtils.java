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

import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.QueryParameter;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.FileHelper;
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
     * Method which handles the action of setting a parameterized value on a
     * statement. Traditionally this is done using the
     * {@link PreparedStatement#setObject(int, Object)} method but for some
     * types we use more specific setter methods.
     * 
     * @param st
     * @param valueIndex
     * @param column
     * @param value
     * @throws SQLException
     */
    public static void setStatementValue(final PreparedStatement st, final int valueIndex, final Column column,
            Object value) throws SQLException {
        final ColumnType type = (column == null ? null : column.getType());

        if (type == null || type == ColumnType.OTHER) {
            // type is not known - nothing more we can do to narrow the type
            st.setObject(valueIndex, value);
            return;
        }

        if (value == null && type != null) {
            try {
                final int jdbcType = type.getJdbcType();
                st.setNull(valueIndex, jdbcType);
                return;
            } catch (Exception e) {
                logger.warn("Exception occurred while calling setNull(...) for value index " + valueIndex
                        + ". Attempting value-based setter method instead.", e);
            }
        }

        if (type == ColumnType.VARCHAR && value instanceof Date) {
            // some drivers (SQLite and JTDS for MS SQL server) treat dates as
            // VARCHARS. In that case we need to convert the dates to the
            // correct format
            String nativeType = column.getNativeType();
            Date date = (Date) value;
            if ("DATE".equalsIgnoreCase(nativeType)) {
                value = FormatHelper.formatSqlTime(ColumnType.DATE, date, false);
            } else if ("TIME".equalsIgnoreCase(nativeType)) {
                value = FormatHelper.formatSqlTime(ColumnType.TIME, date, false);
            } else if ("TIMESTAMP".equalsIgnoreCase(nativeType) || "DATETIME".equalsIgnoreCase(nativeType)) {
                value = FormatHelper.formatSqlTime(ColumnType.TIMESTAMP, date, false);
            }
        }

        if (type != null && type.isTimeBased() && value instanceof String) {
            value = FormatHelper.parseSqlTime(type, (String) value);
        }

        try {
            if (type == ColumnType.DATE && value instanceof Date) {
                Calendar cal = Calendar.getInstance();
                cal.setTime((Date) value);
                st.setDate(valueIndex, new java.sql.Date(cal.getTimeInMillis()), cal);
            } else if (type == ColumnType.TIME && value instanceof Date) {
                final Time time = toTime((Date) value);
                st.setTime(valueIndex, time);
            } else if (type == ColumnType.TIMESTAMP && value instanceof Date) {
                final Timestamp ts = toTimestamp((Date) value);
                st.setTimestamp(valueIndex, ts);
            } else if (type == ColumnType.CLOB || type == ColumnType.NCLOB) {
                if (value instanceof InputStream) {
                    InputStream inputStream = (InputStream) value;
                    st.setAsciiStream(valueIndex, inputStream);
                } else if (value instanceof Reader) {
                    Reader reader = (Reader) value;
                    st.setCharacterStream(valueIndex, reader);
                } else if (value instanceof NClob) {
                    NClob nclob = (NClob) value;
                    st.setNClob(valueIndex, nclob);
                } else if (value instanceof Clob) {
                    Clob clob = (Clob) value;
                    st.setClob(valueIndex, clob);
                } else if (value instanceof String) {
                    st.setString(valueIndex, (String) value);
                } else {
                    st.setObject(valueIndex, value);
                }
            } else if (type == ColumnType.BLOB || type == ColumnType.BINARY) {
                if (value instanceof byte[]) {
                    byte[] bytes = (byte[]) value;
                    st.setBytes(valueIndex, bytes);
                } else if (value instanceof InputStream) {
                    InputStream inputStream = (InputStream) value;
                    st.setBinaryStream(valueIndex, inputStream);
                } else if (value instanceof Blob) {
                    Blob blob = (Blob) value;
                    st.setBlob(valueIndex, blob);
                } else {
                    st.setObject(valueIndex, value);
                }
            } else if (type.isLiteral()) {
                final String str;
                if (value instanceof Reader) {
                    Reader reader = (Reader) value;
                    str = FileHelper.readAsString(reader);
                } else {
                    str = value.toString();
                }
                st.setString(valueIndex, str);
            } else {
                st.setObject(valueIndex, value);
            }
        } catch (SQLException e) {
            logger.error("Failed to set parameter {} to value: {}", valueIndex, value);
            throw e;
        }
    }

    private static Time toTime(Date value) {
        if (value instanceof Time) {
            return (Time) value;
        }
        final Calendar cal = Calendar.getInstance();
        cal.setTime((Date) value);
        return new java.sql.Time(cal.getTimeInMillis());
    }

    private static Timestamp toTimestamp(Date value) {
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        final Calendar cal = Calendar.getInstance();
        cal.setTime((Date) value);
        return new Timestamp(cal.getTimeInMillis());
    }

    public static String getValueAsSql(Column column, Object value, IQueryRewriter queryRewriter) {
        if (value == null) {
            return "NULL";
        }
        final ColumnType columnType = column.getType();
        if (columnType.isLiteral() && value instanceof String) {
            value = queryRewriter.escapeQuotes((String) value);
        }
        String formatSqlValue = FormatHelper.formatSqlValue(columnType, value);
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
