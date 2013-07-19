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
package org.apache.metamodel.jdbc.dialects;

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.Query;

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
