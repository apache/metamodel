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
package org.apache.metamodel.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromClause;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.GroupByItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

/**
 * The QuerySplitter class makes it possible to split up queries that are
 * expected to yield a huge result set which may cause performance problems like
 * OutOfMemoryError's or very long processing periods. The resulting queries
 * will in union produce the same result, but in smaller bits (resultsets with
 * less rows).
 * 
 * Note that there is an initial performance-penalty associated with splitting
 * the query since some queries will be executed in order to determine
 * reasonable intervals to use for the resulting queries WHERE clauses.
 * 
 * @see Query
 * @see DataContext
 */
public final class QuerySplitter {

    public final static long DEFAULT_MAX_ROWS = 300000;
    private static final int MINIMUM_MAX_ROWS = 100;
    private final static Logger logger = LoggerFactory.getLogger(QuerySplitter.class);

    private final Query _query;
    private final DataContext _dataContext;
    private long _maxRows = DEFAULT_MAX_ROWS;
    private Long _cachedRowCount = null;

    public QuerySplitter(DataContext dc, Query q) {
        if (dc == null) {
            throw new IllegalArgumentException("DataContext cannot be null");
        }
        if (q == null) {
            throw new IllegalArgumentException("Query cannot be null");
        }
        _dataContext = dc;
        _query = q;
    }

    /**
     * Splits the query into several queries that will together yield the same
     * result set
     * 
     * @return a list of queries that can be executed to yield the same
     *         collective result as this QuerySplitter's query
     */
    public List<Query> splitQuery() {
        List<Query> result = new ArrayList<Query>();
        if (isSplittable()) {
            if (getRowCount() > _maxRows) {
                Integer subQueryIndex = getSubQueryFromItemIndex();
                List<Query> splitQueries = null;
                if (subQueryIndex != null) {
                    splitQueries = splitQueryBasedOnSubQueries(subQueryIndex);
                } else {
                    List<Column> splitColumns = getSplitColumns();
                    splitQueries = splitQueryBasedOnColumns(splitColumns);
                }
                result.addAll(splitQueries);
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Accepted query, maxRows not exceeded: " + _query);
                }
                result.add(_query);
            }
        }
        if (result.isEmpty()) {
            logger.debug("Cannot further split query: {}", _query);
            result.add(_query);
        }
        return result;
    }

    private List<Query> splitQueryBasedOnColumns(List<Column> splitColumns) {
        List<Query> result = new ArrayList<Query>();
        if (splitColumns.isEmpty() || getRowCount() <= _maxRows) {
            if (getRowCount() > 0) {
                result.add(_query);
            }
        } else {
            Column firstColumn = splitColumns.get(0);
            splitColumns.remove(0);
            List<Query> splitQueries = splitQueryBasedOnColumn(firstColumn);
            for (Query splitQuery : splitQueries) {
                QuerySplitter qs = new QuerySplitter(_dataContext, splitQuery).setMaxRows(_maxRows);
                if (qs.getRowCount() > _maxRows) {
                    // Recursively use the next columns to split queries
                    // subsequently
                    result.addAll(qs.splitQueryBasedOnColumns(splitColumns));
                } else {
                    if (qs.getRowCount() > 0) {
                        result.add(splitQuery);
                    }
                }
            }
        }
        return result;
    }

    private List<Query> splitQueryBasedOnColumn(Column column) {
        SelectItem maxItem = new SelectItem(FunctionType.MAX, column);
        SelectItem minItem = new SelectItem(FunctionType.MIN, column);
        Query q = new Query().from(column.getTable()).select(maxItem, minItem);
        Row row = MetaModelHelper.executeSingleRowQuery(_dataContext, q);
        long max = ceil((Number) row.getValue(maxItem));
        long min = floor((Number) row.getValue(minItem));
        long wholeRange = max - min;
        List<Query> result = new ArrayList<Query>();
        if (wholeRange <= 1) {
            result.add(_query);
        } else {
            long numSplits = ceil(getRowCount() / _maxRows);
            if (numSplits < 2) {
                // Must as a minimum yield two new queries
                numSplits = 2;
            }
            int splitInterval = (int) (wholeRange / numSplits);
            for (int i = 0; i < numSplits; i++) {
                q = _query.clone();
                long lowLimit = min + (i * splitInterval);
                long highLimit = lowLimit + splitInterval;

                FilterItem lowerThanFilter = new FilterItem(new SelectItem(column), OperatorType.LESS_THAN, highLimit);
                FilterItem higherThanFilter = new FilterItem(new SelectItem(column), OperatorType.GREATER_THAN,
                        lowLimit);
                FilterItem equalsFilter = new FilterItem(new SelectItem(column), OperatorType.EQUALS_TO, lowLimit);

                if (i == 0) {
                    // This is the first split query: no higherThan filter and
                    // include
                    // IS NULL
                    FilterItem nullFilter = new FilterItem(new SelectItem(column), OperatorType.EQUALS_TO, null);
                    FilterItem orFilterItem = new FilterItem(lowerThanFilter, nullFilter);
                    q.where(orFilterItem);
                } else if (i + 1 == numSplits) {
                    // This is the lats split query: no lowerThan filter,
                    FilterItem orFilterItem = new FilterItem(higherThanFilter, equalsFilter);
                    q.where(orFilterItem);
                } else {
                    higherThanFilter = new FilterItem(higherThanFilter, equalsFilter);
                    lowerThanFilter = new FilterItem(lowerThanFilter, equalsFilter);
                    q.where(higherThanFilter);
                    q.where(lowerThanFilter);
                }
                result.add(q);
            }
        }
        return result;
    }

    private static long floor(Number value) {
        Double floor = Math.floor(value.doubleValue());
        return floor.longValue();
    }

    private static long ceil(Number value) {
        Double ceil = Math.ceil(value.doubleValue());
        return ceil.longValue();
    }

    private List<Query> splitQueryBasedOnSubQueries(int fromItemIndex) {
        Query subQuery = _query.getFromClause().getItem(fromItemIndex).getSubQuery();
        QuerySplitter subQuerySplitter = new QuerySplitter(_dataContext, subQuery);

        subQuerySplitter.setMaxRows(_maxRows);
        List<Query> splitQueries = subQuerySplitter.splitQuery();
        List<Query> result = new ArrayList<Query>(splitQueries.size());
        for (Query splitQuery : splitQueries) {
            Query newQuery = _query.clone();
            FromClause fromClause = newQuery.getFromClause();
            String alias = fromClause.getItem(fromItemIndex).getAlias();
            fromClause.removeItem(fromItemIndex);
            newQuery.from(new FromItem(splitQuery).setAlias(alias));
            result.add(newQuery);
        }
        return result;
    }

    private Integer getSubQueryFromItemIndex() {
        List<FromItem> fromItems = _query.getFromClause().getItems();
        for (int i = 0; i < fromItems.size(); i++) {
            Query subQuery = fromItems.get(i).getSubQuery();
            if (subQuery != null) {
                if (isSplittable(subQuery)) {
                    return i;
                }
            }
        }
        return null;
    }

    private boolean isSplittable() {
        return isSplittable(_query);
    }

    public static boolean isSplittable(Query q) {
        if (q.getOrderByClause().getItemCount() != 0) {
            return false;
        }
        return true;
    }

    private List<Column> getSplitColumns() {
        List<Column> result = new ArrayList<Column>();
        if (_query.getGroupByClause().getItemCount() != 0) {
            List<GroupByItem> groupByItems = _query.getGroupByClause().getItems();
            for (GroupByItem groupByItem : groupByItems) {
                Column column = groupByItem.getSelectItem().getColumn();
                if (column != null) {
                    if (column.isIndexed()) {
                        // Indexed columns have first priority, they will be
                        // added to the beginning of the list
                        result.add(0, column);
                    } else {
                        result.add(column);
                    }
                }
            }
        } else {
            List<FromItem> fromItems = _query.getFromClause().getItems();
            for (FromItem fromItem : fromItems) {
                if (fromItem.getTable() != null) {
                    addColumnsToResult(fromItem.getTable(), result);
                }
                if (fromItem.getJoin() != null && fromItem.getAlias() == null) {
                    if (fromItem.getLeftSide().getTable() != null) {
                        addColumnsToResult(fromItem.getLeftSide().getTable(), result);
                    }
                    if (fromItem.getRightSide().getTable() != null) {
                        addColumnsToResult(fromItem.getRightSide().getTable(), result);
                    }
                }
            }
        }
        return result;
    }

    private static void addColumnsToResult(Table table, List<Column> result) {
        for (Column column: table.getNumberColumns()) {
            if (column.isIndexed()) {
                // Indexed columns have first priority, they will be
                // added to the beginning of the list
                result.add(0, column);
            } else {
                result.add(column);
            }
        }
    }

    /**
     * @return the total number of rows expected from executing the query.
     */
    public long getRowCount() {
        if (_cachedRowCount == null) {
            _cachedRowCount = getRowCount(_query);
        }
        return _cachedRowCount;
    }

    private long getRowCount(Query q) {
        q = q.clone();
        SelectItem countAllItem = SelectItem.getCountAllItem();
        if (q.getGroupByClause().getItemCount() > 0) {
            q = new Query().from(new FromItem(q).setAlias("sq")).select(countAllItem);
        } else {
            q.getSelectClause().removeItems();
            q.select(countAllItem);
        }
        Row row = MetaModelHelper.executeSingleRowQuery(_dataContext, q);
        Number count = (Number) row.getValue(countAllItem);
        return count.longValue();
    }

    /**
     * Sets the desired maximum result set row count. Note that this size cannot
     * be guaranteed, but will serve as an indicator for determining the
     * split-size
     * 
     * @param maxRows
     */
    public QuerySplitter setMaxRows(long maxRows) {
        if (maxRows < MINIMUM_MAX_ROWS) {
            throw new IllegalArgumentException("maxRows must be higher than " + MINIMUM_MAX_ROWS);
        }
        _maxRows = maxRows;
        return this;
    }

    public DataSet executeQueries() {
        return executeQueries(splitQuery());
    }

    public DataSet executeQueries(List<Query> splitQueries) {
        return new SplitQueriesDataSet(_dataContext, splitQueries);
    }
}