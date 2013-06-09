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
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectClause;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;

/**
 * Query rewriter for HSQLDB
 */
public class HsqldbQueryRewriter extends DefaultQueryRewriter {

    public HsqldbQueryRewriter(JdbcDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public String rewriteColumnType(ColumnType columnType) {
        if (columnType == ColumnType.BLOB) {
            return "LONGVARBINARY";
        }
        return super.rewriteColumnType(columnType);
    }

    @Override
    public boolean isFirstRowSupported() {
        return true;
    }

    @Override
    public boolean isMaxRowsSupported() {
        return true;
    }

    @Override
    protected String rewriteSelectClause(Query query, SelectClause selectClause) {
        String result = super.rewriteSelectClause(query, selectClause);

        Integer firstRow = query.getFirstRow();
        Integer maxRows = query.getMaxRows();
        if (maxRows != null || firstRow != null) {
            if (maxRows == null) {
                maxRows = Integer.MAX_VALUE;
            }
            if (firstRow == null || firstRow <= 0) {
                result = "SELECT TOP " + maxRows + " " + result.substring(7);
            } else {
                final int offset = firstRow - 1;
                result = "SELECT LIMIT " + offset + " " + maxRows + " " + result.substring(7);
            }
        }

        return result;
    }

    @Override
    public String rewriteFilterItem(FilterItem item) {
        if (!item.isCompoundFilter()) {
            final SelectItem selectItem = item.getSelectItem();
            final Column column = selectItem.getColumn();
            if (column != null) {
                if (column.getType() == ColumnType.TIMESTAMP) {
                    // HSQLDB does not treat (TIMESTAMP 'yyyy-MM-dd hh:mm:ss')
                    // tokens correctly
                    String result = super.rewriteFilterItem(item);
                    int indexOfTimestamp = result.lastIndexOf("TIMESTAMP");
                    if (indexOfTimestamp != -1) {
                        result = result.substring(0, indexOfTimestamp)
                                + result.substring(indexOfTimestamp + "TIMESTAMP".length());
                    }
                    return result;
                }
            }
        }
        return super.rewriteFilterItem(item);
    }

}
