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
package org.apache.metamodel.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.query.OrderByItem.Direction;
import org.apache.metamodel.query.parser.QueryParserException;
import org.apache.metamodel.query.parser.QueryPartCollectionProcessor;
import org.apache.metamodel.query.parser.QueryPartParser;
import org.apache.metamodel.query.parser.QueryPartProcessor;
import org.apache.metamodel.query.parser.SelectItemParser;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.BaseObject;
import org.apache.metamodel.util.BooleanComparator;
import org.apache.metamodel.util.FormatHelper;
import org.apache.metamodel.util.NumberComparator;

/**
 * Represents a query to retrieve data by. A query is made up of six clauses,
 * equivalent to the SQL standard:
 * <ul>
 * <li>the SELECT clause, which define the wanted columns of the resulting
 * DataSet</li>
 * <li>the FROM clause, which define where to retrieve the data from</li>
 * <li>the WHERE clause, which define filters on the retrieved data</li>
 * <li>the GROUP BY clause, which define if the result should be grouped and
 * aggregated according to some columns acting as categories</li>
 * <li>the HAVING clause, which define filters on the grouped data</li>
 * <li>the ORDER BY clause, which define sorting of the resulting dataset</li>
 * </ul>
 * 
 * In addition two properties are applied to queries to limit the resulting
 * dataset:
 * <ul>
 * <li>First row: The first row (aka. offset) of the result of the query.</li>
 * <li>Max rows: The maximum amount of rows to return when executing the query.</li>
 * </ul>
 * 
 * Queries are executed using the DataContext.executeQuery method or can
 * alternatively be used directly in JDBC by using the toString() method.
 * 
 * @see DataContext
 */
public final class Query extends BaseObject implements Cloneable, Serializable {

    private static final long serialVersionUID = -5976325207498574216L;

    private final SelectClause _selectClause;
    private final FromClause _fromClause;
    private final FilterClause _whereClause;
    private final GroupByClause _groupByClause;
    private final FilterClause _havingClause;
    private final OrderByClause _orderByClause;

    private Integer _maxRows;
    private Integer _firstRow;

    public Query() {
        _selectClause = new SelectClause(this);
        _fromClause = new FromClause(this);
        _whereClause = new FilterClause(this, AbstractQueryClause.PREFIX_WHERE);
        _groupByClause = new GroupByClause(this);
        _havingClause = new FilterClause(this, AbstractQueryClause.PREFIX_HAVING);
        _orderByClause = new OrderByClause(this);
    }

    public Query select(Column column, FromItem fromItem) {
        SelectItem selectItem = new SelectItem(column, fromItem);
        return select(selectItem);
    }

    public Query select(Column... columns) {
        for (Column column : columns) {
            SelectItem selectItem = new SelectItem(column);
            selectItem.setQuery(this);
            _selectClause.addItem(selectItem);
        }
        return this;
    }

    public Query select(SelectItem... items) {
        _selectClause.addItems(items);
        return this;
    }

    public Query select(FunctionType functionType, Column column) {
        _selectClause.addItem(new SelectItem(functionType, column));
        return this;
    }

    public Query select(String expression, String alias) {
        return select(new SelectItem(expression, alias));
    }

    /**
     * Adds a selection to this query.
     * 
     * @param expression
     * @return
     */
    public Query select(String expression) {
        return select(expression, false);
    }

    /**
     * Adds a selection to this query.
     * 
     * @param expression
     *            a textual representation of the select item, e.g. "MAX(foo)"
     *            or just "foo", where "foo" is a column name.
     * @param allowExpressionBasedSelectItem
     *            whether or not expression-based select items are allowed or
     *            not (see {@link SelectItem#getExpression()}.
     * @return
     */
    public Query select(String expression, boolean allowExpressionBasedSelectItem) {
        final QueryPartParser clauseParser = new QueryPartParser(new SelectItemParser(this,
                allowExpressionBasedSelectItem), expression, ",");
        clauseParser.parse();
        return this;
    }

    private SelectItem findSelectItem(String expression, boolean allowExpressionBasedSelectItem) {
        final SelectItemParser parser = new SelectItemParser(this, allowExpressionBasedSelectItem);
        return parser.findSelectItem(expression);
    }

    /**
     * Select all available select items from all currently available FROM
     * items. Equivalent of the expression "SELECT * FROM ..." in SQL.
     * 
     * @return
     */
    public Query selectAll() {
        List<FromItem> items = getFromClause().getItems();
        for (FromItem fromItem : items) {
            selectAll(fromItem);
        }
        return this;
    }

    public Query selectAll(final FromItem fromItem) {
        if (fromItem.getTable() != null) {
            final Column[] columns = fromItem.getTable().getColumns();
            for (final Column column : columns) {
                select(column, fromItem);
            }
        } else if (fromItem.getJoin() != null) {
            selectAll(fromItem.getLeftSide());
            selectAll(fromItem.getRightSide());
        } else if (fromItem.getSubQuery() != null) {
            final List<SelectItem> items = fromItem.getSubQuery().getSelectClause().getItems();
            for (final SelectItem subQuerySelectItem : items) {
                select(new SelectItem(subQuerySelectItem, fromItem));
            }
        } else {
            throw new MetaModelException("All select items ('*') not determinable with from item: " + fromItem);
        }
        return this;
    }

    public Query selectDistinct() {
        _selectClause.setDistinct(true);
        return this;
    }

    public Query selectCount() {
        return select(SelectItem.getCountAllItem());
    }

    public Query from(FromItem... items) {
        _fromClause.addItems(items);
        return this;
    }

    public Query from(Table table) {
        return from(new FromItem(table));
    }

    public Query from(String expression) {
        return from(new FromItem(expression));
    }

    public Query from(Table table, String alias) {
        return from(new FromItem(table).setAlias(alias));
    }

    public Query from(Table leftTable, Table rightTable, JoinType joinType, Column leftOnColumn, Column rightOnColumn) {
        SelectItem[] leftOn = new SelectItem[] { new SelectItem(leftOnColumn) };
        SelectItem[] rightOn = new SelectItem[] { new SelectItem(rightOnColumn) };
        FromItem fromItem = new FromItem(joinType, new FromItem(leftTable), new FromItem(rightTable), leftOn, rightOn);
        return from(fromItem);
    }

    public Query groupBy(String... groupByTokens) {
        for (String groupByToken : groupByTokens) {
            SelectItem selectItem = findSelectItem(groupByToken, true);
            groupBy(new GroupByItem(selectItem));
        }
        return this;
    }

    public Query groupBy(GroupByItem... items) {
        for (GroupByItem item : items) {
            SelectItem selectItem = item.getSelectItem();
            if (selectItem != null && selectItem.getQuery() == null) {
                selectItem.setQuery(this);
            }
        }
        _groupByClause.addItems(items);
        return this;
    }

    public Query groupBy(Column... columns) {
        for (Column column : columns) {
            SelectItem selectItem = new SelectItem(column).setQuery(this);
            _groupByClause.addItem(new GroupByItem(selectItem));
        }
        return this;
    }

    public Query orderBy(OrderByItem... items) {
        _orderByClause.addItems(items);
        return this;
    }

    public Query orderBy(String... orderByTokens) {
        for (String orderByToken : orderByTokens) {
            orderByToken = orderByToken.trim();
            final Direction direction;
            if (orderByToken.toUpperCase().endsWith("DESC")) {
                direction = Direction.DESC;
                orderByToken = orderByToken.substring(0, orderByToken.length() - 4).trim();
            } else if (orderByToken.toUpperCase().endsWith("ASC")) {
                direction = Direction.ASC;
                orderByToken = orderByToken.substring(0, orderByToken.length() - 3).trim();
            } else {
                direction = Direction.ASC;
            }

            OrderByItem orderByItem = new OrderByItem(findSelectItem(orderByToken, true), direction);
            orderBy(orderByItem);
        }
        return this;
    }

    public Query orderBy(Column column) {
        return orderBy(column, Direction.ASC);
    }

    public Query orderBy(Column column, Direction direction) {
        SelectItem selectItem = _selectClause.getSelectItem(column);
        if (selectItem == null) {
            selectItem = new SelectItem(column);
        }
        return orderBy(new OrderByItem(selectItem, direction));
    }

    public Query where(FilterItem... items) {
        _whereClause.addItems(items);
        return this;
    }

    public Query where(Iterable<FilterItem> items) {
        _whereClause.addItems(items);
        return this;
    }

    public Query where(String... whereItemTokens) {
        for (String whereItemToken : whereItemTokens) {
            FilterItem filterItem = findFilterItem(whereItemToken);
            where(filterItem);
        }
        return this;
    }

    private FilterItem findFilterItem(String expression) {
        String upperExpression = expression.toUpperCase();

        final QueryPartCollectionProcessor collectionProcessor = new QueryPartCollectionProcessor();
        new QueryPartParser(collectionProcessor, expression, " AND ", " OR ").parse();

        final List<String> tokens = collectionProcessor.getTokens();
        final List<String> delims = collectionProcessor.getDelims();
        if (tokens.size() == 1) {
            expression = tokens.get(0);
            upperExpression = expression.toUpperCase();
        } else {
            final LogicalOperator logicalOperator = LogicalOperator.valueOf(delims.get(1).trim());

            final List<FilterItem> filterItems = new ArrayList<FilterItem>();
            for (int i = 0; i < tokens.size(); i++) {
                String token = tokens.get(i);
                FilterItem filterItem = findFilterItem(token);
                filterItems.add(filterItem);
            }
            return new FilterItem(logicalOperator, filterItems);
        }

        OperatorType operator = null;
        String leftSide = null;
        final String rightSide;
        {
            String rightSideCandidate = null;
            final OperatorType[] operators = OperatorType.BUILT_IN_OPERATORS;
            for (OperatorType operatorCandidate : operators) {
                final String searchStr;
                if (operatorCandidate.isSpaceDelimited()) {
                    searchStr = ' ' + operatorCandidate.toSql() + ' ';
                } else {
                    searchStr = operatorCandidate.toSql();
                }
                final int operatorIndex = upperExpression.indexOf(searchStr);
                if (operatorIndex > 0) {
                    operator = operatorCandidate;
                    leftSide = expression.substring(0, operatorIndex).trim();
                    rightSideCandidate = expression.substring(operatorIndex + searchStr.length()).trim();
                    break;
                }
            }

            if (operator == null) {
                // check special cases for IS NULL and IS NOT NULL
                if (expression.endsWith(" IS NOT NULL")) {
                    operator = OperatorType.DIFFERENT_FROM;
                    leftSide = expression.substring(0, expression.lastIndexOf(" IS NOT NULL")).trim();
                    rightSideCandidate = "NULL";
                } else if (expression.endsWith(" IS NULL")) {
                    operator = OperatorType.EQUALS_TO;
                    leftSide = expression.substring(0, expression.lastIndexOf(" IS NULL")).trim();
                    rightSideCandidate = "NULL";
                }
            }

            rightSide = rightSideCandidate;
        }

        if (operator == null) {
            return new FilterItem(expression);
        }

        final SelectItem selectItem = findSelectItem(leftSide, false);
        if (selectItem == null) {
            return new FilterItem(expression);
        }

        final Object operand;
        if (operator == OperatorType.IN) {
            final List<Object> list = new ArrayList<Object>();
            new QueryPartParser(new QueryPartProcessor() {
                @Override
                public void parse(String delim, String itemToken) {
                    Object operand = createOperand(itemToken, selectItem, false);
                    list.add(operand);
                }
            }, rightSide, ",").parse();
            operand = list;
        } else {
            operand = createOperand(rightSide, selectItem, true);
        }

        return new FilterItem(selectItem, operator, operand);
    }

    private Object createOperand(final String token, final SelectItem leftSelectItem, final boolean searchSelectItems) {
        if (token.equalsIgnoreCase("NULL")) {
            return null;
        }

        if (token.startsWith("'") && token.endsWith("'") && token.length() > 2) {
            String stringOperand = token.substring(1, token.length() - 1);
            stringOperand = stringOperand.replaceAll("\\\\'", "'");
            return stringOperand;
        }

        if (searchSelectItems) {
            final SelectItem selectItem = findSelectItem(token, false);
            if (selectItem != null) {
                return selectItem;
            }
        }

        final ColumnType expectedColumnType = leftSelectItem.getExpectedColumnType();
        final Object result;
        if (expectedColumnType == null) {
            // We're assuming number here, but it could also be boolean or a
            // time based type. But anyways, this should not happen since
            // expected column type should be available.
            result = NumberComparator.toNumber(token);
        } else if (expectedColumnType.isBoolean()) {
            result = BooleanComparator.toBoolean(token);
        } else if (expectedColumnType.isTimeBased()) {
            result = FormatHelper.parseSqlTime(expectedColumnType, token);
        } else {
            result = NumberComparator.toNumber(token);
        }

        if (result == null) {
            // shouldn't happen since only "NULL" is parsed as null.
            throw new QueryParserException("Could not parse operand: " + token);
        }

        return result;
    }

    public Query where(SelectItem selectItem, OperatorType operatorType, Object operand) {
        return where(new FilterItem(selectItem, operatorType, operand));
    }

    public Query where(Column column, OperatorType operatorType, Object operand) {
        SelectItem selectItem = _selectClause.getSelectItem(column);
        if (selectItem == null) {
            selectItem = new SelectItem(column);
        }
        return where(selectItem, operatorType, operand);
    }

    public Query having(FilterItem... items) {
        _havingClause.addItems(items);
        return this;
    }

    public Query having(FunctionType function, Column column, OperatorType operatorType, Object operand) {
        SelectItem selectItem = new SelectItem(function, column);
        return having(new FilterItem(selectItem, operatorType, operand));
    }

    public Query having(Column column, OperatorType operatorType, Object operand) {
        SelectItem selectItem = _selectClause.getSelectItem(column);
        if (selectItem == null) {
            selectItem = new SelectItem(column);
        }
        return having(new FilterItem(selectItem, operatorType, operand));
    }

    public Query having(String... havingItemTokens) {
        for (String havingItemToken : havingItemTokens) {
            FilterItem filterItem = findFilterItem(havingItemToken);
            having(filterItem);
        }
        return this;
    }

    @Override
    public String toString() {
        return toSql();
    }

    /*
     * A string representation of this query. This representation will be SQL 99
     * compatible and can thus be used for database queries on databases that
     * meet SQL standards.
     */
    public String toSql() {
        return toSql(false);
    }

    protected String toSql(boolean includeSchemaInColumnPaths) {
        final StringBuilder sb = new StringBuilder();
        sb.append(_selectClause.toSql(includeSchemaInColumnPaths));
        sb.append(_fromClause.toSql(includeSchemaInColumnPaths));
        sb.append(_whereClause.toSql(includeSchemaInColumnPaths));
        sb.append(_groupByClause.toSql(includeSchemaInColumnPaths));
        sb.append(_havingClause.toSql(includeSchemaInColumnPaths));
        sb.append(_orderByClause.toSql(includeSchemaInColumnPaths));
        return sb.toString();
    }

    public SelectClause getSelectClause() {
        return _selectClause;
    }

    public FromClause getFromClause() {
        return _fromClause;
    }

    public FilterClause getWhereClause() {
        return _whereClause;
    }

    public GroupByClause getGroupByClause() {
        return _groupByClause;
    }

    public FilterClause getHavingClause() {
        return _havingClause;
    }

    public OrderByClause getOrderByClause() {
        return _orderByClause;
    }

    /**
     * Sets the maximum number of rows to be queried. If the result of the query
     * yields more rows they should be discarded.
     * 
     * @param maxRows
     *            the number of desired maximum rows. Can be null (default) for
     *            no limits
     * @return this query
     */
    public Query setMaxRows(Integer maxRows) {
        if (maxRows != null) {
            final int maxRowsValue = maxRows.intValue();
            if (maxRowsValue < 0) {
                throw new IllegalArgumentException("Max rows cannot be negative");
            }
        }
        _maxRows = maxRows;
        return this;
    }

    /**
     * @return the number of maximum rows to yield from executing this query or
     *         null if no maximum/limit is set.
     */
    public Integer getMaxRows() {
        return _maxRows;
    }

    /**
     * Sets the first row (aka offset) of the query's result. The row number is
     * 1-based, so setting a first row value of 1 is equivalent to not setting
     * it at all..
     * 
     * @param firstRow
     *            the first row, where 1 is the first row.
     * @return this query
     */
    public Query setFirstRow(Integer firstRow) {
        if (firstRow != null && firstRow.intValue() < 1) {
            throw new IllegalArgumentException("First row cannot be negative or zero");
        }
        _firstRow = firstRow;
        return this;
    }

    /**
     * Gets the first row (aka offset) of the query's result, or null if none is
     * specified. The row number is 1-based, so setting a first row value of 1
     * is equivalent to not setting it at all..
     * 
     * @return the first row (aka offset) of the query's result, or null if no
     *         offset is specified.
     */
    public Integer getFirstRow() {
        return _firstRow;
    }
    
    public InvokableQuery invokable(DataContext dataContext) {
        return new DefaultInvokableQuery(this, dataContext);
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(_maxRows);
        identifiers.add(_selectClause);
        identifiers.add(_fromClause);
        identifiers.add(_whereClause);
        identifiers.add(_groupByClause);
        identifiers.add(_havingClause);
        identifiers.add(_orderByClause);
    }

    @Override
    public Query clone() {
        final Query q = new Query();
        q.setMaxRows(_maxRows);
        q.setFirstRow(_firstRow);
        q.getSelectClause().setDistinct(_selectClause.isDistinct());
        for (FromItem item : _fromClause.getItems()) {
            q.from(item.clone());
        }
        for (SelectItem item : _selectClause.getItems()) {
            q.select(item.clone(q));
        }
        for (FilterItem item : _whereClause.getItems()) {
            q.where(item.clone());
        }
        for (GroupByItem item : _groupByClause.getItems()) {
            q.groupBy(item.clone());
        }
        for (FilterItem item : _havingClause.getItems()) {
            q.having(item.clone());
        }
        for (OrderByItem item : _orderByClause.getItems()) {
            q.orderBy(item.clone());
        }
        return q;
    }
}
