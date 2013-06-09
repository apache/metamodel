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
package org.eobjects.metamodel.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.jdbc.dialects.IQueryRewriter;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.update.AbstractRowUpdationBuilder;
import org.eobjects.metamodel.update.RowUpdationBuilder;
import org.eobjects.metamodel.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RowUpdationBuilder} that issues an SQL UPDATE statement
 * 
 * @author Kasper Sørensen
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
        final PreparedStatement st = _updateCallback.getPreparedStatement(sql, reuseStatement);
        try {
            if (reuseStatement) {
                Column[] columns = getColumns();
                Object[] values = getValues();
                boolean[] explicitNulls = getExplicitNulls();
                int valueCounter = 1;
                for (int i = 0; i < columns.length; i++) {
                    boolean explicitNull = explicitNulls[i];
                    if (values[i] != null || explicitNull) {
                    	JdbcUtils.setStatementValue(st, valueCounter, columns[i], values[i]);
                    	
                        valueCounter++;
                    }
                }

                List<FilterItem> whereItems = getWhereItems();
                for (FilterItem whereItem : whereItems) {
                    if (JdbcUtils.isPreparedParameterCandidate(whereItem)) {
                        final Object operand = whereItem.getOperand();
                        final Column column = whereItem.getSelectItem().getColumn();
                        
						JdbcUtils.setStatementValue(st, valueCounter, column, operand);
                        
                        valueCounter++;
                    }
                }
            }
            _updateCallback.executePreparedStatement(st, reuseStatement);
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
            if (values[i] != null || explicitNulls[i]) {
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
                    sb.append(JdbcUtils.getValueAsSql(columns[i], values[i], _queryRewriter));
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
