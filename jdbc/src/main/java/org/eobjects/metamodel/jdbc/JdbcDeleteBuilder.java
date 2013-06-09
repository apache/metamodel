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
import org.eobjects.metamodel.delete.AbstractRowDeletionBuilder;
import org.eobjects.metamodel.delete.RowDeletionBuilder;
import org.eobjects.metamodel.jdbc.dialects.IQueryRewriter;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RowDeletionBuilder} that issues an SQL DELETE FROM statement
 * 
 * @author Kasper Sørensen
 */
final class JdbcDeleteBuilder extends AbstractRowDeletionBuilder {

    private static final Logger logger = LoggerFactory.getLogger(JdbcDeleteBuilder.class);

    private final JdbcUpdateCallback _updateCallback;
    private final IQueryRewriter _queryRewriter;
    private final boolean _inlineValues;

    public JdbcDeleteBuilder(JdbcUpdateCallback updateCallback, Table table, IQueryRewriter queryRewriter) {
        this(updateCallback, table, queryRewriter, false);
    }

    public JdbcDeleteBuilder(JdbcUpdateCallback updateCallback, Table table, IQueryRewriter queryRewriter,
            boolean inlineValues) {
        super(table);
        _updateCallback = updateCallback;
        _queryRewriter = queryRewriter;
        _inlineValues = inlineValues;
    }

    @Override
    public void execute() throws MetaModelException {
        String sql = createSqlStatement();

        logger.debug("Delete statement created: {}", sql);
        final boolean reuseStatement = !_inlineValues;
        final PreparedStatement st = _updateCallback.getPreparedStatement(sql, reuseStatement);
        try {
            if (reuseStatement) {
                int valueCounter = 1;
                final List<FilterItem> whereItems = getWhereItems();
                for (FilterItem whereItem : whereItems) {
                    if (JdbcUtils.isPreparedParameterCandidate(whereItem)) {
                        Object operand = whereItem.getOperand();
                        st.setObject(valueCounter, operand);
                        valueCounter++;
                    }
                }
            }
            _updateCallback.executePreparedStatement(st, reuseStatement);
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "execute delete statement: " + sql);
        } finally {
            if (_inlineValues) {
                FileHelper.safeClose(st);
            }
        }
    }

    protected String createSqlStatement() {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ");
        sb.append(_queryRewriter.rewriteFromItem(new FromItem(getTable())));
        sb.append(JdbcUtils.createWhereClause(getWhereItems(), _queryRewriter, _inlineValues));
        return sb.toString();
    }

}
