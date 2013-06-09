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

import org.eobjects.metamodel.drop.AbstractTableDropBuilder;
import org.eobjects.metamodel.drop.TableDropBuilder;
import org.eobjects.metamodel.jdbc.dialects.IQueryRewriter;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

/**
 * {@link TableDropBuilder} that issues an SQL DROP TABLE statement
 * 
 * @author Kasper Sørensen
 */
final class JdbcDropTableBuilder extends AbstractTableDropBuilder implements TableDropBuilder {

    private final JdbcUpdateCallback _updateCallback;
    private final IQueryRewriter _queryRewriter;

    public JdbcDropTableBuilder(JdbcUpdateCallback updateCallback, Table table, IQueryRewriter queryRewriter) {
        super(table);
        _updateCallback = updateCallback;
        _queryRewriter = queryRewriter;
    }

    @Override
    public void execute() {
        String sql = createSqlStatement();

        PreparedStatement statement = _updateCallback.getPreparedStatement(sql, false);
        try {
            _updateCallback.executePreparedStatement(statement, false);

            // remove the table reference from the schema
            final Schema schema = getTable().getSchema();
            if (schema instanceof JdbcSchema) {
                ((JdbcSchema) schema).refreshTables();
            }
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "execute drop table statement: " + sql);
        }
    }

    protected String createSqlStatement() {
        FromItem fromItem = new FromItem(getTable());
        String tableLabel = _queryRewriter.rewriteFromItem(fromItem);

        return "DROP TABLE " + tableLabel;
    }

}
