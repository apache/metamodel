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
import org.eobjects.metamodel.query.Query;

/**
 * Query rewriter for databases that support LIMIT and OFFSET keywords for max
 * rows and first row properties.
 */
public abstract class LimitOffsetQueryRewriter extends DefaultQueryRewriter {

    public LimitOffsetQueryRewriter(JdbcDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public final boolean isFirstRowSupported() {
        return true;
    }

    @Override
    public final boolean isMaxRowsSupported() {
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * If the Max rows and/or First row property of the query is set, then we
     * will use the database's LIMIT and OFFSET functions.
     */
    @Override
    public String rewriteQuery(Query query) {
        String queryString = super.rewriteQuery(query);
        Integer maxRows = query.getMaxRows();
        Integer firstRow = query.getFirstRow();
        if (maxRows != null || firstRow != null) {
            if (maxRows == null) {
                maxRows = Integer.MAX_VALUE;
            }
            queryString = queryString + " LIMIT " + maxRows;

            if (firstRow != null && firstRow > 1) {
                // offset is 0-based
                int offset = firstRow - 1;
                queryString = queryString + " OFFSET " + offset;
            }
        }


        return queryString;
    }
}
