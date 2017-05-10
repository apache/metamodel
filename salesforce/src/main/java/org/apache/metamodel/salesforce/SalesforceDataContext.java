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
package org.apache.metamodel.salesforce;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import com.sforce.ws.ConnectorConfig;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.FirstRowDataSet;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.OrderByItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.ws.ConnectionException;

/**
 * A datacontext that uses the Salesforce API.
 * 
 * Metadata about schema structure is explored using 'describe' SOAP web
 * services.
 * 
 * Queries are fired using the SOQL dialect of SQL, see <a href=
 * "http://www.salesforce.com/us/developer/docs/api/Content/sforce_api_calls_soql_select.htm"
 * >SOQL reference</a>.
 */
public class SalesforceDataContext extends QueryPostprocessDataContext implements UpdateableDataContext {

    public static final TimeZone SOQL_TIMEZONE = TimeZone.getTimeZone("UTC");
    public static final String SOQL_DATE_FORMAT_IN = "yyyy-MM-dd";
    public static final String SOQL_DATE_FORMAT_OUT = "yyyy-MM-dd";
    public static final String SOQL_DATE_TIME_FORMAT_IN = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    public static final String SOQL_DATE_TIME_FORMAT_OUT = "yyyy-MM-dd'T'HH:mm:ssZZZ";
    public static final String SOQL_TIME_FORMAT_IN = "HH:mm:ss.SSS";
    public static final String SOQL_TIME_FORMAT_OUT = "HH:mm:ssZZZ";

    private static final Logger logger = LoggerFactory.getLogger(SalesforceDataContext.class);

    private final PartnerConnection _connection;

    public SalesforceDataContext(String endpoint, String username, String password, String securityToken) {
        try {
            ConnectorConfig config = new ConnectorConfig();
            config.setUsername(username);
            config.setPassword(securityToken == null ? password : password + securityToken);
            config.setAuthEndpoint(endpoint);
            config.setServiceEndpoint(endpoint);
            _connection = Connector.newConnection(config);
        } catch (ConnectionException e) {
            throw SalesforceUtils.wrapException(e, "Failed to log in to Salesforce service");
        }
    }

    public SalesforceDataContext(String username, String password, String securityToken) {
        try {
            _connection = Connector.newConnection(username, securityToken == null ? password : password + securityToken);
        } catch (ConnectionException e) {
            throw SalesforceUtils.wrapException(e, "Failed to log in to Salesforce service");
        }
    }

    public SalesforceDataContext(String username, String password) {
        try {
            _connection = Connector.newConnection(username, password);
        } catch (ConnectionException e) {
            throw SalesforceUtils.wrapException(e, "Failed to log in to Salesforce service");
        }
    }

    /**
     * Creates a {@code SalesforceDataContext} instance , configured with given
     * salesforce connection.
     * 
     * @param connection
     *            salesforce connection (cannot be {@code null}).
     * 
     */
    public SalesforceDataContext(PartnerConnection connection) {
        if (connection == null) {
            throw new IllegalArgumentException("connection cannot be null");
        }
        _connection = connection;
    }

    /**
     * Returns the Salesforce connection being used by this datacontext.
     * 
     * @return the Salesforce connection
     */
    public PartnerConnection getConnection() {
        return _connection;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final SalesforceSchema schema = new SalesforceSchema(getMainSchemaName(), _connection);
        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return "Salesforce";
    }

    @Override
    public DataSet executeQuery(Query query) {
        final List<FromItem> fromItems = query.getFromClause().getItems();
        if (fromItems.size() != 1) {
            // not a simple SELECT ... FROM [table] ... query, we need to use
            // query post processing.
            return super.executeQuery(query);
        }

        final Table table = fromItems.get(0).getTable();
        if (table == null) {
            return super.executeQuery(query);
        }

        if (!query.getGroupByClause().isEmpty()) {
            return super.executeQuery(query);
        }

        if (!query.getHavingClause().isEmpty()) {
            return super.executeQuery(query);
        }

        final List<SelectItem> selectItems = query.getSelectClause().getItems();
        final StringBuilder sb = new StringBuilder();

        try {
            sb.append("SELECT ");
            int i = 0;
            final Column[] columns = new Column[selectItems.size()];
            for (SelectItem selectItem : selectItems) {
                validateSoqlSupportedSelectItem(selectItem);
                columns[i] = selectItem.getColumn();
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(columns[i].getName());
                i++;
            }

            sb.append(" FROM ");
            sb.append(table.getName());

            boolean firstWhere = true;
            for (FilterItem filterItem : query.getWhereClause().getItems()) {
                if (firstWhere) {
                    sb.append(" WHERE ");
                    firstWhere = false;
                } else {
                    sb.append(" AND ");
                }
                rewriteFilterItem(sb, filterItem);
            }

            i = 0;
            final List<OrderByItem> items = query.getOrderByClause().getItems();
            for (OrderByItem orderByItem : items) {
                if (i == 0) {
                    sb.append(" ORDER BY ");
                } else {
                    sb.append(", ");
                }

                final SelectItem selectItem = orderByItem.getSelectItem();
                validateSoqlSupportedSelectItem(selectItem);

                final Column column = selectItem.getColumn();
                sb.append(column.getName());
                sb.append(' ');
                sb.append(orderByItem.getDirection());

                i++;
            }

            final Integer firstRow = query.getFirstRow();
            final Integer maxRows = query.getMaxRows();
            if (maxRows != null && maxRows > 0) {
                if (firstRow != null) {
                    // add first row / offset to avoid missing some records.
                    sb.append(" LIMIT " + (maxRows + firstRow - 1));
                } else {
                    sb.append(" LIMIT " + maxRows);
                }
            }

            final QueryResult result = executeSoqlQuery(sb.toString());

            DataSet dataSet = new SalesforceDataSet(columns, result, _connection);

            if (firstRow != null) {
                // OFFSET is still only a developer preview feature of SFDC. See
                // http://www.salesforce.com/us/developer/docs/api/Content/sforce_api_calls_soql_select_offset.htm
                dataSet = new FirstRowDataSet(dataSet, firstRow.intValue());
            }

            return dataSet;

        } catch (UnsupportedOperationException e) {
            logger.debug("Failed to rewrite query to SOQL, falling back to regular query post-processing", e);
            return super.executeQuery(query);
        }
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        final String query;

        try {
            final StringBuilder sb = new StringBuilder();
            sb.append("SELECT COUNT() FROM ");
            sb.append(table.getName());

            boolean firstWhere = true;
            for (FilterItem filterItem : whereItems) {
                if (firstWhere) {
                    sb.append(" WHERE ");
                    firstWhere = false;
                } else {
                    sb.append(" AND ");
                }
                rewriteFilterItem(sb, filterItem);
            }
            query = sb.toString();
        } catch (UnsupportedOperationException e) {
            logger.debug("Failed to rewrite count query, falling back to client side counting", e);

            // unable to rewrite to SOQL, counting will be done client side.
            return null;
        }

        final QueryResult queryResult = executeSoqlQuery(query);

        assert queryResult.isDone();

        return queryResult.getSize();
    }

    protected static void rewriteFilterItem(StringBuilder sb, FilterItem filterItem)
            throws UnsupportedOperationException {
        if (filterItem.isCompoundFilter()) {
            FilterItem[] childrend = filterItem.getChildItems();
            boolean firstChild = true;
            sb.append('(');
            for (FilterItem child : childrend) {
                if (firstChild) {
                    firstChild = false;
                } else {
                    sb.append(' ');
                    sb.append(filterItem.getLogicalOperator().toString());
                    sb.append(' ');
                }
                rewriteFilterItem(sb, child);
            }
            sb.append(')');
            return;
        }

        final SelectItem selectItem = filterItem.getSelectItem();
        validateSoqlSupportedSelectItem(selectItem);

        final Column column = selectItem.getColumn();
        sb.append(column.getName());
        sb.append(' ');

        final OperatorType operator = filterItem.getOperator();
        if (OperatorType.IN.equals(operator)) {
            throw new UnsupportedOperationException("IN operator not supported: " + filterItem);
        }
        sb.append(operator.toSql());
        sb.append(' ');

        final Object operand = filterItem.getOperand();
        if (operand == null) {
            sb.append("null");
        } else if (operand instanceof String) {
            sb.append('\'');

            String str = operand.toString();
            str = str.replaceAll("\'", "\\\\'");
            str = str.replaceAll("\"", "\\\\\"");
            str = str.replaceAll("\r", "\\\\r");
            str = str.replaceAll("\n", "\\\\n");
            str = str.replaceAll("\t", "\\\\t");

            sb.append(str);
            sb.append('\'');
        } else if (operand instanceof Number) {
            sb.append(operand);
        } else if (operand instanceof Date) {
            final SimpleDateFormat dateFormat;
            ColumnType expectedColumnType = selectItem.getExpectedColumnType();
            if (expectedColumnType == ColumnType.DATE) {
                // note: we don't apply the timezone for DATE fields, since they
                // don't contain time-of-day information.
                dateFormat = new SimpleDateFormat(SOQL_DATE_FORMAT_OUT);
            } else if (expectedColumnType == ColumnType.TIME) {
                dateFormat = new SimpleDateFormat(SOQL_TIME_FORMAT_OUT, Locale.ENGLISH);
                dateFormat.setTimeZone(SOQL_TIMEZONE);
            } else {
                dateFormat = new SimpleDateFormat(SOQL_DATE_TIME_FORMAT_OUT, Locale.ENGLISH);
                dateFormat.setTimeZone(SOQL_TIMEZONE);
            }

            String str = dateFormat.format((Date) operand);
            logger.debug("Date '{}' formatted as: {}", operand, str);
            sb.append(str);
        } else if (operand instanceof Column) {
            sb.append(((Column) operand).getName());
        } else if (operand instanceof SelectItem) {
            SelectItem operandSelectItem = (SelectItem) operand;
            validateSoqlSupportedSelectItem(operandSelectItem);
            sb.append(operandSelectItem.getColumn().getName());
        } else {
            throw new UnsupportedOperationException("Unsupported operand: " + operand);
        }
    }

    private static void validateSoqlSupportedSelectItem(SelectItem selectItem) throws UnsupportedOperationException {
        if (selectItem.getFunction() != null) {
            throw new UnsupportedOperationException("Function select items not supported: " + selectItem);
        }
        if (selectItem.getSubQuerySelectItem() != null) {
            throw new UnsupportedOperationException("Subquery select items not supported: " + selectItem);
        }
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        final StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (int i = 0; i < columns.length; i++) {
            if (i != 0) {
                sb.append(',');
            }
            sb.append(columns[i].getName());
        }
        sb.append(" FROM ");
        sb.append(table.getName());

        if (maxRows > 0) {
            sb.append(" LIMIT " + maxRows);
        }

        final QueryResult queryResult = executeSoqlQuery(sb.toString());
        return new SalesforceDataSet(columns, queryResult, _connection);
    }

    private QueryResult executeSoqlQuery(String query) {
        logger.info("Executing SOQL query: {}", query);
        try {
            QueryResult queryResult = _connection.query(query);
            return queryResult;
        } catch (ConnectionException e) {
            throw SalesforceUtils.wrapException(e, "Failed to invoke query service");
        }
    }

    @Override
    public void executeUpdate(UpdateScript update) {
        final SalesforceUpdateCallback callback = new SalesforceUpdateCallback(this, _connection);
        update.run(callback);
        callback.close();
    }
}
