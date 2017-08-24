/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.metamodel.jdbc.dialects;

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.Query;

/**
 * Query rewriter for databases that support OFFSET and FETCH keywords for max
 * rows and first row properties.
 */
public abstract class OffsetFetchQueryRewriter extends DefaultQueryRewriter {

    private final String _databaseProductName;
    private final int _databaseSupportedVersion;
    private final boolean _fetchNeedsOffsetAndOrderBy;

    public OffsetFetchQueryRewriter(final JdbcDataContext dataContext, final int minSupportedVersion,
            final boolean fetchNeedsOrderBy) {
        super(dataContext);
        _databaseProductName = dataContext.getDatabaseProductName();
        _databaseSupportedVersion = minSupportedVersion;
        _fetchNeedsOffsetAndOrderBy = fetchNeedsOrderBy;
    }

    @Override
    public boolean isFirstRowSupported(final Query query) {
        return isSupportedVersion(_databaseProductName, _databaseSupportedVersion) && !query.getOrderByClause()
                .isEmpty();
    }

    @Override
    public boolean isMaxRowsSupported() {
        return isSupportedVersion(_databaseProductName, _databaseSupportedVersion);
    }

    /**
     * {@inheritDoc}
     *
     * If the Max rows and First row property of the query is set, then we
     * will use the database's "OFFSET i ROWS FETCH NEXT j ROWS ONLY" construct.
     */
    @Override
    public String rewriteQuery(final Query query) {
        final boolean hasOrderBy = !query.getOrderByClause().isEmpty();
        String queryString = super.rewriteQuery(query);

        if (isSupportedVersion(_databaseProductName, _databaseSupportedVersion) && (query.getMaxRows() != null
                || query.getFirstRow() != null)) {
            final Integer maxRows = query.getMaxRows();
            Integer firstRow = query.getFirstRow();

            if (!_fetchNeedsOffsetAndOrderBy || hasOrderBy) {
                if (firstRow != null) {
                    queryString = queryString + " OFFSET " + (firstRow - 1) + " ROWS";
                } else if (_fetchNeedsOffsetAndOrderBy) {
                    // TOP should do it.
                    return queryString;
                }

                if (maxRows != null) {
                    queryString = queryString.replaceAll(" TOP [0-9]+", "");
                    queryString += " FETCH NEXT " + maxRows + " ROWS ONLY";
                }
            }
        }
        return queryString;
    }
}
