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

package org.eobjects.metamodel.jdbc.dialects;

import org.eobjects.metamodel.jdbc.JdbcDataContext;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

/**
 * Query rewriter for PostgreSQL
 */
public class PostgresqlQueryRewriter extends LimitOffsetQueryRewriter implements IQueryRewriter {

    public PostgresqlQueryRewriter(JdbcDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public ColumnType getColumnType(int jdbcType, String nativeType, Integer columnSize) {
        if ("bool".equals(nativeType)) {
            // override the normal behaviour of postgresql which maps "bool" to
            // a BIT.
            return ColumnType.BOOLEAN;
        }
        return super.getColumnType(jdbcType, nativeType, columnSize);
    }

    @Override
    public String rewriteColumnType(ColumnType columnType) {
        if (columnType == ColumnType.BLOB) {
            return "bytea";
        }
        return super.rewriteColumnType(columnType);
    }

    @Override
    protected String rewriteFromItem(Query query, FromItem item) {
        String result = super.rewriteFromItem(query, item);
        Table table = item.getTable();
        if (table != null) {
            Schema schema = table.getSchema();
            if (schema != null) {
                String schemaName = schema.getName();
                if (schemaName != null) {
                    result = result.replaceFirst(schemaName, '\"' + schema.getName() + '\"');
                }
            }
        }
        return result;
    }
}