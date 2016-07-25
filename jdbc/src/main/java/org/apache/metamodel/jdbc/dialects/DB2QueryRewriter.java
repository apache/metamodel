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

import java.util.Date;
import java.util.List;

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.util.FormatHelper;
import org.apache.metamodel.util.TimeComparator;

/**
 * Query rewriter for IBM DB2
 */
public class DB2QueryRewriter extends DefaultQueryRewriter {

    public DB2QueryRewriter(JdbcDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public String escapeQuotes(String filterItemOperand) {
        return filterItemOperand.replaceAll("\\'", "\\\\'");
    }

    /**
     * DB2 expects the fully qualified column name, including schema, in select
     * items.
     */
    @Override
    public boolean isSchemaIncludedInColumnPaths() {
        return true;
    }

    @Override
    public boolean isMaxRowsSupported() {
        return true;
    }

    @Override
    public boolean isFirstRowSupported() {
        return true;
    }

    @Override
    public String rewriteQuery(Query query) {
        final Integer firstRow = query.getFirstRow();
        final Integer maxRows = query.getMaxRows();

        if (maxRows == null && (firstRow == null || firstRow.intValue() == 1)) {
            return super.rewriteQuery(query);
        }

        if ((firstRow == null || firstRow.intValue() == 1) && maxRows != null && maxRows > 0) {
            // We prefer to use the "FETCH FIRST [n] ROWS ONLY" approach, if
            // firstRow is not specified.
            return super.rewriteQuery(query) + " FETCH FIRST " + maxRows + " ROWS ONLY";

        } else {
            // build a ROW_NUMBER() query like this:

            // SELECT [original select clause]
            // FROM ([original select clause],
            // ROW_NUMBER() AS metamodel_row_number
            // FROM [remainder of regular query])
            // WHERE metamodel_row_number BETWEEN [firstRow] and [maxRows];

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
    }

    @Override
    public String rewriteColumnType(ColumnType columnType, Integer columnSize) {
        if (columnType == ColumnType.BOOLEAN || columnType == ColumnType.BIT) {
            return "SMALLINT";
        }
        return super.rewriteColumnType(columnType, columnSize);
    }

    @Override
    public String rewriteFilterItem(FilterItem item) {
        final SelectItem selectItem = item.getSelectItem();
        final Object itemOperand = item.getOperand();
        final OperatorType operator = item.getOperator();
        if (null != selectItem && itemOperand != null) {
            ColumnType columnType = selectItem.getExpectedColumnType();
            if (columnType != null) {
                if (columnType.isTimeBased()) {
                    // special logic for DB2 based time operands.

                    StringBuilder sb = new StringBuilder();
                    sb.append(selectItem.getSameQueryAlias(true));
                    final Object operand = FilterItem.appendOperator(sb, itemOperand, operator);

                    if (operand instanceof SelectItem) {
                        final String selectItemString = ((SelectItem) operand).getSameQueryAlias(true);
                        sb.append(selectItemString);
                    } else {
                        Date date = TimeComparator.toDate(itemOperand);
                        if (date == null) {
                            throw new IllegalStateException("Could not convert " + itemOperand + " to date");
                        }

                        final String sqlValue = FormatHelper.formatSqlTime(columnType, date, true, "('", "')");
                        sb.append(sqlValue);
                    }

                    return sb.toString();
                }
            }
        }
        return super.rewriteFilterItem(item);
    }

}