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

import java.util.Date;
import java.util.List;

import org.eobjects.metamodel.jdbc.JdbcDataContext;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.query.OperatorType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.util.FormatHelper;
import org.eobjects.metamodel.util.TimeComparator;

/**
 * Query rewriter for IBM DB2
 */
public class DB2QueryRewriter extends DefaultQueryRewriter implements IQueryRewriter {

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

        if (maxRows == null && firstRow == null) {
            return super.rewriteQuery(query);
        }

        if (firstRow == null || firstRow.intValue() == 1) {
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

            innerQuery.select(new SelectItem("ROW_NUMBER() OVER()", "metamodel_row_number"));

            final String baseQueryString = rewriteQuery(outerQuery);

            if (maxRows == null) {
                return baseQueryString + " WHERE metamodel_row_number > " + (firstRow - 1);
            }

            return baseQueryString + " WHERE metamodel_row_number BETWEEN " + firstRow + " AND " + (firstRow - 1 + maxRows);
        }
    }

    @Override
    public String rewriteColumnType(ColumnType columnType) {
        switch (columnType) {
        case BOOLEAN:
        case BIT:
            return "SMALLINT";
        default:
            return super.rewriteColumnType(columnType);
        }
    }

    @Override
    public String rewriteFilterItem(FilterItem item) {
        SelectItem _selectItem = item.getSelectItem();
        Object _operand = item.getOperand();
        OperatorType _operator = item.getOperator();
        if (null != _selectItem && _operand != null) {
            ColumnType columnType = _selectItem.getExpectedColumnType();
            if (columnType != null) {
                if (columnType.isTimeBased()) {
                    // special logic for DB2 based time operands.

                    StringBuilder sb = new StringBuilder();
                    sb.append(_selectItem.getSameQueryAlias(true));
                    final Object operand = FilterItem.appendOperator(sb, _operand, _operator);

                    if (operand instanceof SelectItem) {
                        final String selectItemString = ((SelectItem) operand).getSameQueryAlias(true);
                        sb.append(selectItemString);
                    } else {
                        Date date = TimeComparator.toDate(_operand);
                        if (date == null) {
                            throw new IllegalStateException("Could not convert " + _operand + " to date");
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