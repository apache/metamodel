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

import java.util.List;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;

/**
 * Query rewriter for databases that support RowNumber keywords for max
 * rows and first row properties.
 **/

public class RowNumberQueryRewriter extends DefaultQueryRewriter {

    public RowNumberQueryRewriter(JdbcDataContext dataContext) {
        super(dataContext);
    }

    protected String getRowNumberSql(Query query, Integer maxRows, Integer firstRow) {
        final Query innerQuery = query.clone();
        innerQuery.setFirstRow(null);
        innerQuery.setMaxRows(null);

        final Query outerQuery = new Query();
        final FromItem subQuerySelectItem = new FromItem(innerQuery).setAlias("metamodel_subquery");
        outerQuery.from(subQuerySelectItem);

        final List<SelectItem> innerSelectItems = innerQuery.getSelectClause().getItems();
        for (SelectItem selectItem : innerSelectItems) {
            outerQuery.select(new SelectItem(selectItem, subQuerySelectItem));
        }


        final String rewrittenOrderByClause = rewriteOrderByClause(innerQuery, innerQuery.getOrderByClause());
        final String rowOver = "ROW_NUMBER() OVER(" + rewrittenOrderByClause + ")";
        innerQuery.select(new SelectItem(rowOver, "metamodel_row_number"));
        innerQuery.getOrderByClause().removeItems();

        final String baseQueryString = rewriteQuery(outerQuery);

        if (maxRows == null) {
            return baseQueryString + " WHERE metamodel_row_number > " + (firstRow - 1);
        }

        return baseQueryString + " WHERE metamodel_row_number BETWEEN " + firstRow + " AND "
                + (firstRow - 1 + maxRows);
    }

}
