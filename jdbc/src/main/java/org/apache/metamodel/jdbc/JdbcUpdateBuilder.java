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
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.AbstractRowUpdationBuilder;
import org.apache.metamodel.update.RowUpdationBuilder;
import org.apache.metamodel.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RowUpdationBuilder} that issues an SQL UPDATE statement
 */
final class JdbcUpdateBuilder extends AbstractRowUpdationBuilder {

    private static final Logger logger = LoggerFactory.getLogger(JdbcUpdateBuilder.class);

    private final boolean _inlineValues;
    private final JdbcUpdateCallback _updateCallback;
    private final IQueryRewriter _queryRewriter;

    public JdbcUpdateBuilder(JdbcUpdateCallback updateCallback, Table table, IQueryRewriter queryRewriter) {
        this(updateCallback, table, queryRewriter, false);
    }

    public JdbcUpdateBuilder(JdbcUpdateCallback updateCallback, Table table, IQueryRewriter queryRewriter,
            boolean inlineValues) {
        super(table);
        _updateCallback = updateCallback;
        _queryRewriter = queryRewriter;
        _inlineValues = inlineValues;
    }

    @Override
    public void execute() throws MetaModelException {
        String sql = createSqlStatement();
        logger.debug("Update statement created: {}", sql);
        final boolean reuseStatement = !_inlineValues;
        final PreparedStatement st = _updateCallback.getPreparedStatement(sql, reuseStatement, false);
        try {
            if (reuseStatement) {
                Column[] columns = getColumns();
                Object[] values = getValues();
                boolean[] explicitNulls = getExplicitNulls();
                int valueCounter = 1;
                for (int i = 0; i < columns.length; i++) {
                    boolean explicitNull = explicitNulls[i];
                    if (values[i] != null || explicitNull) {
                        _queryRewriter.setStatementParameter(st, valueCounter, columns[i], values[i]);

                        valueCounter++;
                    }
                }

                List<FilterItem> whereItems = getWhereItems();
                for (FilterItem whereItem : whereItems) {
                    if (JdbcUtils.isPreparedParameterCandidate(whereItem)) {
                        final Object operand = whereItem.getOperand();
                        final Column column = whereItem.getSelectItem().getColumn();

                        _queryRewriter.setStatementParameter(st, valueCounter, column, operand);

                        valueCounter++;
                    }
                }
            }
            _updateCallback.executeUpdate(st, reuseStatement);
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "execute update statement: " + sql);
        } finally {
            if (_inlineValues) {
                FileHelper.safeClose(st);
            }
        }
    }

    protected String createSqlStatement() {
        return createSqlStatement(_inlineValues);
    }

    private String createSqlStatement(boolean inlineValues) {
        final Object[] values = getValues();
        final Table table = getTable();
        final StringBuilder sb = new StringBuilder();

        final String tableLabel = _queryRewriter.rewriteFromItem(new FromItem(table));

        sb.append("UPDATE ");
        sb.append(tableLabel);
        sb.append(" SET ");

        Column[] columns = getColumns();
        boolean[] explicitNulls = getExplicitNulls();
        boolean firstValue = true;
        for (int i = 0; i < columns.length; i++) {
            final Object value = values[i];
            if (value != null || explicitNulls[i]) {
                if (firstValue) {
                    firstValue = false;
                } else {
                    sb.append(',');
                }
                String columnName = columns[i].getName();
                columnName = _updateCallback.quoteIfNescesary(columnName);
                sb.append(columnName);

                sb.append('=');
                if (inlineValues) {
                    sb.append(JdbcUtils.getValueAsSql(columns[i], value, _queryRewriter));
                } else {
                    sb.append('?');
                }
            }
        }

        sb.append(JdbcUtils.createWhereClause(getWhereItems(), _queryRewriter, inlineValues));
        String sql = sb.toString();
        return sql;
    }

    @Override
    public String toSql() {
        return createSqlStatement(true);
    }
}
