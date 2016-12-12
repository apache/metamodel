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
package org.apache.metamodel.jdbc.dialects;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.AbstractQueryClause;
import org.apache.metamodel.query.FilterClause;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromClause;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.GroupByClause;
import org.apache.metamodel.query.GroupByItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.OrderByClause;
import org.apache.metamodel.query.OrderByItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.ScalarFunction;
import org.apache.metamodel.query.SelectClause;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ColumnTypeImpl;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.FormatHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of query rewriter. This implementation delegates the
 * rewriting of the Query into several subtasks according to the query items to
 * be rendered. This makes it easy to overload single methods in order to
 * correct syntax quirks.
 */
public abstract class AbstractQueryRewriter implements IQueryRewriter {

    private static final Logger logger = LoggerFactory.getLogger(AbstractQueryRewriter.class);

    private final JdbcDataContext _dataContext;

    public AbstractQueryRewriter(JdbcDataContext dataContext) {
        _dataContext = dataContext;
    }

    public JdbcDataContext getDataContext() {
        return _dataContext;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public ColumnType getColumnType(int jdbcType, String nativeType, Integer columnSize) {
        return ColumnTypeImpl.convertColumnType(jdbcType);
    }

    public String rewriteQuery(Query query) {
        query = beforeRewrite(query);

        final StringBuilder sb = new StringBuilder();
        sb.append(rewriteSelectClause(query, query.getSelectClause()));
        sb.append(rewriteFromClause(query, query.getFromClause()));
        sb.append(rewriteWhereClause(query, query.getWhereClause()));
        sb.append(rewriteGroupByClause(query, query.getGroupByClause()));
        sb.append(rewriteHavingClause(query, query.getHavingClause()));
        sb.append(rewriteOrderByClause(query, query.getOrderByClause()));
        return sb.toString();
    }

    public boolean isSchemaIncludedInColumnPaths() {
        return false;
    }

    /**
     * Method to modify query before rewriting begins. Overwrite this method if
     * you want to change parts of the query that are not just rendering
     * related. Cloning the query before modifying is recommended in order to
     * not violate referential integrity of clients (the query is mutable).
     * 
     * @param query
     * @return the modified query
     */
    protected Query beforeRewrite(Query query) {
        return query;
    }

    @Override
    public String rewriteColumnType(ColumnType columnType, Integer columnSize) {
        return rewriteColumnTypeInternal(columnType.toString(), columnSize);
    }

    protected String rewriteColumnTypeInternal(String columnType, Object columnParameter) {
        final StringBuilder sb = new StringBuilder();
        sb.append(columnType);
        if (columnParameter != null) {
            sb.append('(');
            sb.append(columnParameter);
            sb.append(')');
        }
        return sb.toString();
    }

    protected String rewriteOrderByClause(Query query, OrderByClause orderByClause) {
        StringBuilder sb = new StringBuilder();
        if (orderByClause.getItemCount() > 0) {
            sb.append(AbstractQueryClause.PREFIX_ORDER_BY);
            List<OrderByItem> items = orderByClause.getItems();
            for (int i = 0; i < items.size(); i++) {
                OrderByItem item = items.get(i);
                if (i != 0) {
                    sb.append(AbstractQueryClause.DELIM_COMMA);
                }
                sb.append(rewriteOrderByItem(query, item));
            }
        }
        return sb.toString();
    }

    @Override
    public String rewriteFromItem(FromItem item) {
        return rewriteFromItem(item.getQuery(), item);
    }

    protected String rewriteOrderByItem(Query query, OrderByItem item) {
        return item.toSql(isSchemaIncludedInColumnPaths());
    }

    protected String rewriteHavingClause(Query query, FilterClause havingClause) {
        StringBuilder sb = new StringBuilder();
        if (havingClause.getItemCount() > 0) {
            sb.append(AbstractQueryClause.PREFIX_HAVING);
            List<FilterItem> items = havingClause.getItems();
            for (int i = 0; i < items.size(); i++) {
                FilterItem item = items.get(i);
                if (i != 0) {
                    sb.append(AbstractQueryClause.DELIM_AND);
                }
                sb.append(rewriteFilterItem(item));
            }
        }
        return sb.toString();
    }

    protected String rewriteGroupByClause(Query query, GroupByClause groupByClause) {
        StringBuilder sb = new StringBuilder();
        if (groupByClause.getItemCount() > 0) {
            sb.append(AbstractQueryClause.PREFIX_GROUP_BY);
            List<GroupByItem> items = groupByClause.getItems();
            for (int i = 0; i < items.size(); i++) {
                GroupByItem item = items.get(i);
                if (i != 0) {
                    sb.append(AbstractQueryClause.DELIM_COMMA);
                }
                sb.append(rewriteGroupByItem(query, item));
            }
        }
        return sb.toString();
    }

    protected String rewriteGroupByItem(Query query, GroupByItem item) {
        return item.toSql(isSchemaIncludedInColumnPaths());
    }

    protected String rewriteWhereClause(Query query, FilterClause whereClause) {
        StringBuilder sb = new StringBuilder();
        if (whereClause.getItemCount() > 0) {
            sb.append(AbstractQueryClause.PREFIX_WHERE);
            List<FilterItem> items = whereClause.getItems();
            for (int i = 0; i < items.size(); i++) {
                FilterItem item = items.get(i);
                if (i != 0) {
                    sb.append(AbstractQueryClause.DELIM_AND);
                }
                sb.append(rewriteFilterItem(item));
            }
        }
        return sb.toString();
    }

    @Override
    public String rewriteFilterItem(FilterItem item) {
        if (item.isCompoundFilter()) {
            FilterItem[] childItems = item.getChildItems();
            StringBuilder sb = new StringBuilder();
            sb.append('(');
            for (int i = 0; i < childItems.length; i++) {
                FilterItem child = childItems[i];
                if (i != 0) {
                    sb.append(' ');
                    sb.append(item.getLogicalOperator().toString());
                    sb.append(' ');
                }
                sb.append(rewriteFilterItem(child));
            }
            sb.append(')');
            return sb.toString();
        }

        final String primaryFilterSql = item.toSql(isSchemaIncludedInColumnPaths());

        final OperatorType operator = item.getOperator();
        if (OperatorType.DIFFERENT_FROM.equals(operator)) {
            final Object operand = item.getOperand();
            if (operand != null) {
                // special case in SQL where NULL is not treated as a value -
                // see Ticket #1058

                FilterItem isNullFilter = new FilterItem(item.getSelectItem(), OperatorType.EQUALS_TO, null);
                final String secondaryFilterSql = rewriteFilterItem(isNullFilter);

                return '(' + primaryFilterSql + " OR " + secondaryFilterSql + ')';
            }
        }

        return primaryFilterSql;
    }

    protected String rewriteFromClause(Query query, FromClause fromClause) {
        StringBuilder sb = new StringBuilder();
        if (fromClause.getItemCount() > 0) {
            sb.append(AbstractQueryClause.PREFIX_FROM);
            List<FromItem> items = fromClause.getItems();
            for (int i = 0; i < items.size(); i++) {
                FromItem item = items.get(i);
                if (i != 0) {
                    sb.append(AbstractQueryClause.DELIM_COMMA);
                }
                sb.append(rewriteFromItem(query, item));
            }
        }
        return sb.toString();
    }

    protected String rewriteFromItem(Query query, FromItem item) {
        return item.toSql(isSchemaIncludedInColumnPaths());
    }

    protected String rewriteSelectClause(Query query, SelectClause selectClause) {
        StringBuilder sb = new StringBuilder();
        if (selectClause.getItemCount() > 0) {
            sb.append(AbstractQueryClause.PREFIX_SELECT);
            if (selectClause.isDistinct()) {
                sb.append("DISTINCT ");
            }
            List<SelectItem> items = selectClause.getItems();
            for (int i = 0; i < items.size(); i++) {
                SelectItem item = items.get(i);
                if (i != 0) {
                    sb.append(AbstractQueryClause.DELIM_COMMA);
                }

                final ScalarFunction scalarFunction = item.getScalarFunction();
                if (scalarFunction != null && !isScalarFunctionSupported(scalarFunction)) {
                    // replace with a SelectItem without the function - the
                    // function will be applied in post-processing.
                    item = item.replaceFunction(null);
                }

                sb.append(rewriteSelectItem(query, item));
            }
        }
        return sb.toString();
    }

    protected String rewriteSelectItem(Query query, SelectItem item) {
        if (item.isFunctionApproximationAllowed()) {
            // function approximation is not included in any standard SQL
            // constructions - will have to be overridden by subclasses if there
            // are specialized dialects for it.
            item = item.replaceFunctionApproximationAllowed(false);
        }
        return item.toSql(isSchemaIncludedInColumnPaths());
    }
    
    @Override
    public void setStatementParameter(PreparedStatement st, int valueIndex, Column column, Object value)
            throws SQLException {

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
    
    protected Time toTime(Date value) {
        if (value instanceof Time) {
            return (Time) value;
        }
        final Calendar cal = Calendar.getInstance();
        cal.setTime((Date) value);
        return new java.sql.Time(cal.getTimeInMillis());
    }

    protected Timestamp toTimestamp(Date value) {
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        final Calendar cal = Calendar.getInstance();
        cal.setTime((Date) value);
        return new Timestamp(cal.getTimeInMillis());
    }
    
    @Override
    public Object getResultSetValue(ResultSet resultSet, int columnIndex, Column column) throws SQLException {
        final ColumnType type = column.getType();
        try {
            if (type == ColumnType.TIME) {
                return resultSet.getTime(columnIndex);
            } else if (type == ColumnType.DATE) {
                return resultSet.getDate(columnIndex);
            } else if (type == ColumnType.TIMESTAMP) {
                return resultSet.getTimestamp(columnIndex);
            } else if (type == ColumnType.BLOB) {
                final Blob blob = resultSet.getBlob(columnIndex);
                return blob;
            } else if (type == JdbcDataContext.COLUMN_TYPE_BLOB_AS_BYTES) {
                final Blob blob = resultSet.getBlob(columnIndex);
                final InputStream inputStream = blob.getBinaryStream();
                final byte[] bytes = FileHelper.readAsBytes(inputStream);
                return bytes;
            } else if (type.isBinary()) {
                return resultSet.getBytes(columnIndex);
            } else if (type == ColumnType.CLOB || type == ColumnType.NCLOB) {
                final Clob clob = resultSet.getClob(columnIndex);
                return clob;
            } else if (type == JdbcDataContext.COLUMN_TYPE_CLOB_AS_STRING) {
                final Clob clob = resultSet.getClob(columnIndex);
                final Reader reader = clob.getCharacterStream();
                final String result = FileHelper.readAsString(reader);
                return result;
            } else if (type.isBoolean()) {
                return resultSet.getBoolean(columnIndex);
            }
        } catch (Exception e) {
            logger.warn("Failed to retrieve " + type
                    + " value using type-specific getter, retrying with generic getObject(...) method", e);
        }
        return resultSet.getObject(columnIndex);
    }

    protected boolean isSupportedVersion(String databaseProductName, int databaseVersion) {

        if(databaseProductName.equals(_dataContext.getDatabaseProductName())
                && databaseVersion <= getDatabaseMajorVersion(_dataContext.getDatabaseVersion())) {
            return true;
        }
        return false;
    }

    private int getDatabaseMajorVersion(String version) {
        int firstDot = -1;
        if(version != null) {
            version = version.replaceAll("[^0-9.]+", "");
            firstDot = version.indexOf('.');
        }
        if(firstDot >= 0) {
            return Integer.valueOf(version.substring(0, firstDot));
        } else {
            return 0;
        }
    }
}