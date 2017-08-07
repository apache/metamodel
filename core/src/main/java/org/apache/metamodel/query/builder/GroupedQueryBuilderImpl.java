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
package org.apache.metamodel.query.builder;

import java.util.Arrays;
import java.util.List;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.CompiledQuery;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.ScalarFunction;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.query.parser.SelectItemParser;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.BaseObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main implementation of the {@link GroupedQueryBuilder} interface.
 */
final class GroupedQueryBuilderImpl extends BaseObject implements GroupedQueryBuilder {

    private static final Logger logger = LoggerFactory.getLogger(GroupedQueryBuilderImpl.class);

    private final Query _query;
    private final DataContext _dataContext;

    public GroupedQueryBuilderImpl(DataContext dataContext, Query query) {
        if (query == null) {
            throw new IllegalArgumentException("query cannot be null");
        }
        _dataContext = dataContext;
        _query = query;
    }

    @Override
    public ColumnSelectBuilder<GroupedQueryBuilder> select(Column column) {
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }
        return new ColumnSelectBuilderImpl(column, _query, this);
    }

    @Override
    public SatisfiedQueryBuilder<?> select(FunctionType function, String columnName) {
        if (function == null) {
            throw new IllegalArgumentException("function cannot be null");
        }
        final Column column = findColumn(columnName);
        return new FunctionSelectBuilderImpl(function, column, null, _query, this);
    }

    @Override
    public FunctionSelectBuilder<GroupedQueryBuilder> select(FunctionType function, Column column) {
        if (function == null) {
            throw new IllegalArgumentException("function cannot be null");
        }
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }
        return new FunctionSelectBuilderImpl(function, column, null, _query, this);
    }

    @Override
    public SatisfiedQueryBuilder<GroupedQueryBuilder> where(FilterItem... filters) {
        _query.where(filters);
        return this;
    }

    @Override
    public SatisfiedQueryBuilder<GroupedQueryBuilder> where(Iterable<FilterItem> filters) {
        _query.where(filters);
        return this;
    }

    @Override
    public ColumnSelectBuilder<GroupedQueryBuilder> select(String columnName) {
        final Column column = findColumn(columnName);
        return select(column);
    }

    @Override
    public CountSelectBuilder<GroupedQueryBuilder> selectCount() {
        return new CountSelectBuilderImpl(_query, this);
    }

    @Override
    public SatisfiedSelectBuilder<GroupedQueryBuilder> select(Column... columns) {
        if (columns == null) {
            throw new IllegalArgumentException("column cannot be null");
        }
        _query.select(columns);
        return new SatisfiedSelectBuilderImpl(this);
    }

    @Override
    public WhereBuilder<GroupedQueryBuilder> where(Column column) {
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }
        return new WhereBuilderImpl(column, _query, this);
    }

    @Override
    public WhereBuilder<GroupedQueryBuilder> where(String columnName) {
        final Column column = findColumn(columnName);
        return where(column);
    }

    @Override
    public WhereBuilder<GroupedQueryBuilder> where(ScalarFunction function, Column column) {
        final SelectItem selectItem = new SelectItem(function, column);
        return new WhereBuilderImpl(selectItem, _query, this);
    }

    @Override
    public WhereBuilder<GroupedQueryBuilder> where(ScalarFunction function, String columnName) {
        final Column column = findColumn(columnName);
        return where(function, column);
    }

    @Override
    public Column findColumn(final String columnName) throws IllegalArgumentException {
        if (columnName == null) {
            throw new IllegalArgumentException("columnName cannot be null");
        }

        final List<FromItem> fromItems = _query.getFromClause().getItems();
        final List<SelectItem> selectItems = _query.getSelectClause().getItems();

        int dotIndex = columnName.indexOf('.');
        if (dotIndex != -1) {
            // check aliases of from items
            final String aliasPart = columnName.substring(0, dotIndex);
            final String columnPart = columnName.substring(dotIndex + 1);

            for (FromItem fromItem : fromItems) {
                Column column = null;
                column = findColumnInAliasedTable(column, fromItem, aliasPart, columnPart);
                if (column != null) {
                    return column;
                }
            }
        }

        // check columns already in select clause
        for (SelectItem item : selectItems) {
            Column column = item.getColumn();
            if (column != null) {
                if (columnName.equals(column.getName())) {
                    return column;
                }
            }
        }

        for (FromItem fromItem : fromItems) {
            Table table = fromItem.getTable();
            if (table != null) {
                Column column = table.getColumnByName(columnName);
                if (column != null) {
                    return column;
                }
            }
        }

        Column column = _dataContext.getColumnByQualifiedLabel(columnName);
        if (column != null) {
            return column;
        }

        final IllegalArgumentException exception = new IllegalArgumentException("Could not find column: " + columnName);

        if (logger.isDebugEnabled()) {
            logger.debug("findColumn('" + columnName + "') could not resolve a column", exception);
            for (FromItem fromItem : fromItems) {
                final Table table = fromItem.getTable();
                if (table != null) {
                    logger.debug("Table available in FROM item: {}. Column names: {}", table, Arrays.toString(table
                            .getColumnNames().toArray()));
                }
            }
        }

        throw exception;
    }

    private Column findColumnInAliasedTable(Column column, FromItem fromItem, String aliasPart, String columnPart) {
        if (column != null) {
            // ensure that if the column has already been found, return it
            return column;
        }

        Table table = fromItem.getTable();
        if (table != null) {
            String alias = fromItem.getAlias();
            if (alias != null && alias.equals(aliasPart)) {
                column = table.getColumnByName(columnPart);
            }
        } else {
            FromItem leftSide = fromItem.getLeftSide();
            column = findColumnInAliasedTable(column, leftSide, aliasPart, columnPart);
            FromItem rightSide = fromItem.getRightSide();
            column = findColumnInAliasedTable(column, rightSide, aliasPart, columnPart);
            if (column != null) {
                Query subQuery = fromItem.getSubQuery();
                if (subQuery != null) {
                    List<FromItem> items = subQuery.getFromClause().getItems();
                    for (FromItem subQueryFromItem : items) {
                        column = findColumnInAliasedTable(column, subQueryFromItem, aliasPart, columnPart);
                    }
                }
            }
        }

        return column;
    }

    @Override
    public SatisfiedOrderByBuilder<GroupedQueryBuilder> orderBy(String columnName) {
        return orderBy(findColumn(columnName));
    }

    @Override
    public SatisfiedOrderByBuilder<GroupedQueryBuilder> orderBy(Column column) {
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }
        return new SatisfiedOrderByBuilderImpl(column, _query, this);
    }

    @Override
    public SatisfiedOrderByBuilder<GroupedQueryBuilder> orderBy(FunctionType function, Column column) {
        if (function == null) {
            throw new IllegalArgumentException("function cannot be null");
        }
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }
        return new SatisfiedOrderByBuilderImpl(function, column, _query, this);
    }

    @Override
    public GroupedQueryBuilder groupBy(Column column) {
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }
        _query.groupBy(column);
        return this;
    }

    @Override
    public GroupedQueryBuilder groupBy(String columnName) {
        final Column column = findColumn(columnName);
        return groupBy(column);
    }

    @Override
    public GroupedQueryBuilder groupBy(String... columnNames) {
        _query.groupBy(columnNames);
        return this;
    }

    @Override
    public GroupedQueryBuilder groupBy(Column... columns) {
        if (columns == null) {
            throw new IllegalArgumentException("columns cannot be null");
        }
        _query.groupBy(columns);
        return this;
    }

    @Override
    public HavingBuilder having(String columnExpression) {
        final SelectItemParser parser = new SelectItemParser(_query, false);
        final SelectItem selectItem = parser.findSelectItem(columnExpression);
        return having(selectItem);
    }
    
    @Override
    public HavingBuilder having(SelectItem selectItem) {
        if (selectItem == null) {
            throw new IllegalArgumentException("selectItem cannot be null");
        }
        return new HavingBuilderImpl(selectItem, _query, this);
    }

    @Override
    public HavingBuilder having(FunctionType function, Column column) {
        if (function == null) {
            throw new IllegalArgumentException("function cannot be null");
        }
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }
        return new HavingBuilderImpl(function, column, _query, this);
    }

    @Override
    public SatisfiedQueryBuilder<GroupedQueryBuilder> limit(int maxRows) {
        _query.setMaxRows(maxRows);
        return this;
    }

    @Override
    public SatisfiedQueryBuilder<GroupedQueryBuilder> maxRows(int maxRows) {
        _query.setMaxRows(maxRows);
        return this;
    }

    @Override
    public SatisfiedQueryBuilder<GroupedQueryBuilder> firstRow(int firstRow) {
        if (firstRow >= 0) {
            _query.setFirstRow(firstRow);
        } else {
            _query.setFirstRow(null);
        }
        return this;
    }

    @Override
    public SatisfiedQueryBuilder<GroupedQueryBuilder> offset(int offset) {
        if (offset >= 0) {
            _query.setFirstRow(offset + 1);
        } else {
            _query.setFirstRow(null);
        }
        return this;
    }

    @Override
    public String toString() {
        return _query.toSql();
    }

    @Override
    public Query toQuery() {
        return _query.clone();
    }

    @Override
    public CompiledQuery compile() {
        return _dataContext.compileQuery(_query);
    }

    @Override
    public DataSet execute() {
        return _dataContext.executeQuery(_query);
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(_query);
    }
}