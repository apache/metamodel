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
package org.eobjects.metamodel.salesforce;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.QueryPostprocessDataContext;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.FirstRowDataSet;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.query.OperatorType;
import org.eobjects.metamodel.query.OrderByItem;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
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

    public static final String SOQL_DATE_FORMAT_IN = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    public static final String SOQL_DATE_FORMAT_OUT = "yyyy-MM-dd'T'HH:mm:ssZZZ";

    private static final Logger logger = LoggerFactory.getLogger(SalesforceDataContext.class);

    private final PartnerConnection _connection;

    public SalesforceDataContext(String username, String password, String securityToken) {
        try {
            _connection = Connector.newConnection(username, password + securityToken);
        } catch (ConnectionException e) {
            throw SalesforceUtils.wrapException(e, "Failed to log in to Salesforce service");
        }
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
        if (operator == OperatorType.IN) {
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
            SimpleDateFormat dateFormat = new SimpleDateFormat(SOQL_DATE_FORMAT_OUT);
            String str = dateFormat.format((Date) operand);
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
