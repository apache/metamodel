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
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.ColumnType;

/**
 * Query rewriter for Apache Hive
 */
public class HiveQueryRewriter extends DefaultQueryRewriter {

    public HiveQueryRewriter(JdbcDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public String rewriteQuery(Query query) {

        Integer maxRows = query.getMaxRows();
        Integer firstRow = query.getFirstRow();

        if(firstRow != null && firstRow > 1) {
            if(query.getOrderByClause().getItemCount() == 0) {
                throw new MetaModelException("OFFSET requires an ORDER BY clause");
            }
        }

        if (maxRows == null && (firstRow == null || firstRow.intValue() == 1)) {
            return super.rewriteQuery(query);
        }

        if ((firstRow == null || firstRow.intValue() == 1) && maxRows != null && maxRows > 0) {
            // We prefer to use the "LIMIT n" approach, if
            // firstRow is not specified.
            return super.rewriteQuery(query) + " LIMIT " + maxRows;
        }

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
        final String rowOver = "ROW_NUMBER() " + "OVER(" + rewrittenOrderByClause + ")";
        innerQuery.select(new SelectItem(rowOver, "metamodel_row_number"));
        innerQuery.getOrderByClause().removeItems();

        final String baseQueryString = rewriteQuery(outerQuery);

        if (maxRows == null) {
            return baseQueryString + " WHERE metamodel_row_number > " + (firstRow - 1);
        }

        return baseQueryString + " WHERE metamodel_row_number BETWEEN " + firstRow + " AND "
                + (firstRow - 1 + maxRows);

    }


    @Override
    public String rewriteColumnType(ColumnType columnType, Integer columnSize) {
        if (columnType == ColumnType.INTEGER) {
            return "INT";
        }

        if(columnType == ColumnType.STRING) {
            return "STRING";
        }

        // Hive does not support VARCHAR without a width, nor VARCHAR(MAX).
        // Returning max allowable column size instead.
        if (columnType == ColumnType.VARCHAR && columnSize == null) {
            return super.rewriteColumnType(columnType, 65535);
        }
        return super.rewriteColumnType(columnType, columnSize);
    }
    
    @Override
    public boolean isTransactional() {
        return false;
    }
    
	@Override
	public boolean isPrimaryKeySupported() {
		return false;
	}
}
