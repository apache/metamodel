package org.apache.metamodel.jdbc.dialects;

import java.util.List;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;

/**
 * @author lixiaobao
 * @create 2019-02-01 12:01 AM
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
