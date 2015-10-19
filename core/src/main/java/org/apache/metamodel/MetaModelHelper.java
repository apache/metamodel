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
package org.apache.metamodel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.metamodel.data.CachingDataSetHeader;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.EmptyDataSet;
import org.apache.metamodel.data.FilteredDataSet;
import org.apache.metamodel.data.FirstRowDataSet;
import org.apache.metamodel.data.IRowFilter;
import org.apache.metamodel.data.InMemoryDataSet;
import org.apache.metamodel.data.MaxRowsDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.ScalarFunctionDataSet;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.data.SubSelectionDataSet;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.GroupByItem;
import org.apache.metamodel.query.OrderByItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.ScalarFunction;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.query.parser.QueryParser;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.SuperColumnType;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.AggregateBuilder;
import org.apache.metamodel.util.CollectionUtils;
import org.apache.metamodel.util.Func;
import org.apache.metamodel.util.ObjectComparator;
import org.apache.metamodel.util.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains various helper functionality to common tasks in
 * MetaModel, eg.:
 * 
 * <ul>
 * <li>Easy-access for traversing common schema items</li>
 * <li>Manipulate data in memory. These methods are primarily used to enable
 * queries for non-queryable data sources like CSV files and spreadsheets.</li>
 * <li>Query rewriting, traversing and manipulation.</li>
 * </ul>
 * 
 * The class is mainly intended for internal use within the framework
 * operations, but is kept stable, so it can also be used by framework users.
 */
public final class MetaModelHelper {

    private final static Logger logger = LoggerFactory.getLogger(MetaModelHelper.class);

    private MetaModelHelper() {
        // Prevent instantiation
    }

    /**
     * Creates an array of tables where all occurences of tables in the provided
     * list of tables and columns are included
     */
    public static Table[] getTables(Collection<Table> tableList, Iterable<Column> columnList) {
        HashSet<Table> set = new HashSet<Table>();
        set.addAll(tableList);
        for (Column column : columnList) {
            set.add(column.getTable());
        }
        return set.toArray(new Table[set.size()]);
    }

    /**
     * Determines if a schema is an information schema
     * 
     * @param schema
     * @return
     */
    public static boolean isInformationSchema(Schema schema) {
        String name = schema.getName();
        return isInformationSchema(name);
    }

    /**
     * Determines if a schema name is the name of an information schema
     * 
     * @param name
     * @return
     */
    public static boolean isInformationSchema(String name) {
        if (name == null) {
            return false;
        }
        return QueryPostprocessDataContext.INFORMATION_SCHEMA_NAME.equals(name.toLowerCase());
    }

    /**
     * Converts a list of columns to a corresponding array of tables
     * 
     * @param columns
     *            the columns that the tables will be extracted from
     * @return an array containing the tables of the provided columns.
     */
    public static Table[] getTables(Iterable<Column> columns) {
        ArrayList<Table> result = new ArrayList<Table>();
        for (Column column : columns) {
            Table table = column.getTable();
            if (!result.contains(table)) {
                result.add(table);
            }
        }
        return result.toArray(new Table[result.size()]);
    }

    /**
     * Creates a subset array of columns, where only columns that are contained
     * within the specified table are included.
     * 
     * @param table
     * @param columns
     * @return an array containing the columns that exist in the table
     */
    public static Column[] getTableColumns(Table table, Iterable<Column> columns) {
        if (table == null) {
            return new Column[0];
        }
        final List<Column> result = new ArrayList<Column>();
        for (Column column : columns) {
            final boolean sameTable = table.equals(column.getTable());
            if (sameTable) {
                result.add(column);
            }
        }
        return result.toArray(new Column[result.size()]);
    }

    /**
     * Creates a subset array of columns, where only columns that are contained
     * within the specified table are included.
     * 
     * @param table
     * @param columns
     * @return an array containing the columns that exist in the table
     */
    public static Column[] getTableColumns(Table table, Column[] columns) {
        return getTableColumns(table, Arrays.asList(columns));
    }

    public static DataSet getCarthesianProduct(DataSet... fromDataSets) {
        return getCarthesianProduct(fromDataSets, new FilterItem[0]);
    }

    public static DataSet getCarthesianProduct(DataSet[] fromDataSets, Iterable<FilterItem> whereItems) {
        // First check if carthesian product is even nescesary
        if (fromDataSets.length == 1) {
            return getFiltered(fromDataSets[0], whereItems);
        }

        List<SelectItem> selectItems = new ArrayList<SelectItem>();
        for (DataSet dataSet : fromDataSets) {
            for (int i = 0; i < dataSet.getSelectItems().length; i++) {
                SelectItem item = dataSet.getSelectItems()[i];
                selectItems.add(item);
            }
        }

        int selectItemOffset = 0;
        List<Object[]> data = new ArrayList<Object[]>();
        for (int fromDataSetIndex = 0; fromDataSetIndex < fromDataSets.length; fromDataSetIndex++) {
            DataSet fromDataSet = fromDataSets[fromDataSetIndex];
            SelectItem[] fromSelectItems = fromDataSet.getSelectItems();
            if (fromDataSetIndex == 0) {
                while (fromDataSet.next()) {
                    Object[] values = fromDataSet.getRow().getValues();
                    Object[] row = new Object[selectItems.size()];
                    System.arraycopy(values, 0, row, selectItemOffset, values.length);
                    data.add(row);
                }
                fromDataSet.close();
            } else {
                List<Object[]> fromDataRows = new ArrayList<Object[]>();
                while (fromDataSet.next()) {
                    fromDataRows.add(fromDataSet.getRow().getValues());
                }
                fromDataSet.close();
                for (int i = 0; i < data.size(); i = i + fromDataRows.size()) {
                    Object[] originalRow = data.get(i);
                    data.remove(i);
                    for (int j = 0; j < fromDataRows.size(); j++) {
                        Object[] newRow = fromDataRows.get(j);
                        System.arraycopy(newRow, 0, originalRow, selectItemOffset, newRow.length);
                        data.add(i + j, originalRow.clone());
                    }
                }
            }
            selectItemOffset += fromSelectItems.length;
        }

        if (data.isEmpty()) {
            return new EmptyDataSet(selectItems);
        }

        final DataSetHeader header = new CachingDataSetHeader(selectItems);
        final List<Row> rows = new ArrayList<Row>(data.size());
        for (Object[] objects : data) {
            rows.add(new DefaultRow(header, objects, null));
        }

        DataSet result = new InMemoryDataSet(header, rows);
        if (whereItems != null) {
            DataSet filteredResult = getFiltered(result, whereItems);
            result = filteredResult;
        }
        return result;
    }

    public static DataSet getCarthesianProduct(DataSet[] fromDataSets, FilterItem... filterItems) {
        return getCarthesianProduct(fromDataSets, Arrays.asList(filterItems));
    }

    public static DataSet getFiltered(DataSet dataSet, Iterable<FilterItem> filterItems) {
        List<IRowFilter> filters = CollectionUtils.map(filterItems, new Func<FilterItem, IRowFilter>() {
            @Override
            public IRowFilter eval(FilterItem filterItem) {
                return filterItem;
            }
        });
        if (filters.isEmpty()) {
            return dataSet;
        }

        return new FilteredDataSet(dataSet, filters.toArray(new IRowFilter[filters.size()]));
    }

    public static DataSet getFiltered(DataSet dataSet, FilterItem... filterItems) {
        return getFiltered(dataSet, Arrays.asList(filterItems));
    }

    public static DataSet getSelection(final List<SelectItem> selectItems, final DataSet dataSet) {
        final List<SelectItem> dataSetSelectItems = Arrays.asList(dataSet.getSelectItems());

        // check if the selection is already the same
        if (selectItems.equals(dataSetSelectItems)) {
            // return the DataSet unmodified
            return dataSet;
        }

        final List<SelectItem> scalarFunctionSelectItemsToEvaluate = new ArrayList<>();

        for (SelectItem selectItem : selectItems) {
            if (selectItem.getScalarFunction() != null) {
                if (!dataSetSelectItems.contains(selectItem)
                        && dataSetSelectItems.contains(selectItem.replaceFunction(null))) {
                    scalarFunctionSelectItemsToEvaluate.add(selectItem);
                }
            }
        }

        if (scalarFunctionSelectItemsToEvaluate.isEmpty()) {
            return new SubSelectionDataSet(selectItems, dataSet);
        }

        final ScalarFunctionDataSet scalaFunctionDataSet = new ScalarFunctionDataSet(
                scalarFunctionSelectItemsToEvaluate, dataSet);
        return new SubSelectionDataSet(selectItems, scalaFunctionDataSet);
    }

    public static DataSet getSelection(SelectItem[] selectItems, DataSet dataSet) {
        return getSelection(Arrays.asList(selectItems), dataSet);
    }

    public static DataSet getGrouped(List<SelectItem> selectItems, DataSet dataSet,
            Collection<GroupByItem> groupByItems) {
        return getGrouped(selectItems, dataSet, groupByItems.toArray(new GroupByItem[groupByItems.size()]));
    }

    public static DataSet getGrouped(List<SelectItem> selectItems, DataSet dataSet, GroupByItem[] groupByItems) {
        DataSet result = dataSet;
        if (groupByItems != null && groupByItems.length > 0) {
            Map<Row, Map<SelectItem, List<Object>>> uniqueRows = new HashMap<Row, Map<SelectItem, List<Object>>>();

            final SelectItem[] groupBySelects = new SelectItem[groupByItems.length];
            for (int i = 0; i < groupBySelects.length; i++) {
                groupBySelects[i] = groupByItems[i].getSelectItem();
            }
            final DataSetHeader groupByHeader = new CachingDataSetHeader(groupBySelects);

            // Creates a list of SelectItems that have functions
            List<SelectItem> functionItems = getFunctionSelectItems(selectItems);

            // Loop through the dataset and identify groups
            while (dataSet.next()) {
                Row row = dataSet.getRow();

                // Subselect a row prototype with only the unique values that
                // define the group
                Row uniqueRow = row.getSubSelection(groupByHeader);

                // function input is the values used for calculating aggregate
                // functions in the group
                Map<SelectItem, List<Object>> functionInput;
                if (!uniqueRows.containsKey(uniqueRow)) {
                    // If this group already exist, use an existing function
                    // input
                    functionInput = new HashMap<SelectItem, List<Object>>();
                    for (SelectItem item : functionItems) {
                        functionInput.put(item, new ArrayList<Object>());
                    }
                    uniqueRows.put(uniqueRow, functionInput);
                } else {
                    // If this is a new group, create a new function input
                    functionInput = uniqueRows.get(uniqueRow);
                }

                // Loop through aggregate functions to check for validity
                for (SelectItem item : functionItems) {
                    List<Object> objects = functionInput.get(item);
                    Column column = item.getColumn();
                    if (column != null) {
                        Object value = row.getValue(new SelectItem(column));
                        objects.add(value);
                    } else if (SelectItem.isCountAllItem(item)) {
                        // Just use the empty string, since COUNT(*) don't
                        // evaluate values (but null values should be prevented)
                        objects.add("");
                    } else {
                        throw new IllegalArgumentException("Expression function not supported: " + item);
                    }
                }
            }

            dataSet.close();
            final List<Row> resultData = new ArrayList<Row>();
            final DataSetHeader resultHeader = new CachingDataSetHeader(selectItems);

            // Loop through the groups to generate aggregates
            for (Entry<Row, Map<SelectItem, List<Object>>> entry : uniqueRows.entrySet()) {
                Row row = entry.getKey();
                Map<SelectItem, List<Object>> functionInput = entry.getValue();
                Object[] resultRow = new Object[selectItems.size()];
                // Loop through select items to generate a row
                int i = 0;
                for (SelectItem item : selectItems) {
                    int uniqueRowIndex = row.indexOf(item);
                    if (uniqueRowIndex != -1) {
                        // If there's already a value for the select item in the
                        // row, keep it (it's one of the grouped by columns)
                        resultRow[i] = row.getValue(uniqueRowIndex);
                    } else {
                        // Use the function input to calculate the aggregate
                        // value
                        List<Object> objects = functionInput.get(item);
                        if (objects != null) {
                            Object functionResult = item.getAggregateFunction().evaluate(objects.toArray());
                            resultRow[i] = functionResult;
                        } else {
                            if (item.getAggregateFunction() != null) {
                                logger.error("No function input found for SelectItem: {}", item);
                            }
                        }
                    }
                    i++;
                }
                resultData.add(new DefaultRow(resultHeader, resultRow, null));
            }

            if (resultData.isEmpty()) {
                result = new EmptyDataSet(selectItems);
            } else {
                result = new InMemoryDataSet(resultHeader, resultData);
            }
        }
        result = getSelection(selectItems, result);
        return result;
    }

    /**
     * Applies aggregate values to a dataset. This method is to be invoked AFTER
     * any filters have been applied.
     * 
     * @param workSelectItems
     *            all select items included in the processing of the query
     *            (including those originating from other clauses than the
     *            SELECT clause).
     * @param dataSet
     * @return
     */
    public static DataSet getAggregated(List<SelectItem> workSelectItems, DataSet dataSet) {
        final List<SelectItem> functionItems = getAggregateFunctionSelectItems(workSelectItems);
        if (functionItems.isEmpty()) {
            return dataSet;
        }

        final Map<SelectItem, AggregateBuilder<?>> aggregateBuilders = new HashMap<SelectItem, AggregateBuilder<?>>();
        for (SelectItem item : functionItems) {
            aggregateBuilders.put(item, item.getAggregateFunction().createAggregateBuilder());
        }

        final DataSetHeader header;
        final boolean onlyAggregates;
        if (functionItems.size() != workSelectItems.size()) {
            onlyAggregates = false;
            header = new CachingDataSetHeader(workSelectItems);
        } else {
            onlyAggregates = true;
            header = new SimpleDataSetHeader(workSelectItems);
        }

        final List<Row> resultRows = new ArrayList<Row>();
        while (dataSet.next()) {
            final Row inputRow = dataSet.getRow();
            for (SelectItem item : functionItems) {
                final AggregateBuilder<?> aggregateBuilder = aggregateBuilders.get(item);
                final Column column = item.getColumn();
                if (column != null) {
                    Object value = inputRow.getValue(new SelectItem(column));
                    aggregateBuilder.add(value);
                } else if (SelectItem.isCountAllItem(item)) {
                    // Just use the empty string, since COUNT(*) don't
                    // evaluate values (but null values should be prevented)
                    aggregateBuilder.add("");
                } else {
                    throw new IllegalArgumentException("Expression function not supported: " + item);
                }
            }

            // If the result should also contain non-aggregated values, we
            // will keep those in the rows list
            if (!onlyAggregates) {
                final Object[] values = new Object[header.size()];
                for (int i = 0; i < header.size(); i++) {
                    final Object value = inputRow.getValue(header.getSelectItem(i));
                    if (value != null) {
                        values[i] = value;
                    }
                }
                resultRows.add(new DefaultRow(header, values));
            }
        }
        dataSet.close();

        // Collect the aggregates
        Map<SelectItem, Object> functionResult = new HashMap<SelectItem, Object>();
        for (SelectItem item : functionItems) {
            AggregateBuilder<?> aggregateBuilder = aggregateBuilders.get(item);
            Object result = aggregateBuilder.getAggregate();
            functionResult.put(item, result);
        }

        // if there are no result rows (no matching records at all), we still
        // need to return a record with the aggregates
        final boolean noResultRows = resultRows.isEmpty();

        if (onlyAggregates || noResultRows) {
            // We will only create a single row with all the aggregates
            Object[] values = new Object[header.size()];
            for (int i = 0; i < header.size(); i++) {
                values[i] = functionResult.get(header.getSelectItem(i));
            }
            Row row = new DefaultRow(header, values);
            resultRows.add(row);
        } else {
            // We will create the aggregates as well as regular values
            for (int i = 0; i < resultRows.size(); i++) {
                Row row = resultRows.get(i);
                Object[] values = row.getValues();
                for (Entry<SelectItem, Object> entry : functionResult.entrySet()) {
                    SelectItem item = entry.getKey();
                    int itemIndex = row.indexOf(item);
                    if (itemIndex != -1) {
                        Object value = entry.getValue();
                        values[itemIndex] = value;
                    }
                }
                resultRows.set(i, new DefaultRow(header, values));
            }
        }

        return new InMemoryDataSet(header, resultRows);
    }

    /**
     * 
     * @param selectItems
     * @return
     * 
     * @deprecated use {@link #getAggregateFunctionSelectItems(Iterable)} or
     *             {@link #getScalarFunctionSelectItems(Iterable)} instead
     */
    @Deprecated
    public static List<SelectItem> getFunctionSelectItems(Iterable<SelectItem> selectItems) {
        return CollectionUtils.filter(selectItems, new Predicate<SelectItem>() {
            @Override
            public Boolean eval(SelectItem arg) {
                return arg.getFunction() != null;
            }
        });
    }

    public static List<SelectItem> getAggregateFunctionSelectItems(Iterable<SelectItem> selectItems) {
        return CollectionUtils.filter(selectItems, new Predicate<SelectItem>() {
            @Override
            public Boolean eval(SelectItem arg) {
                return arg.getAggregateFunction() != null;
            }
        });
    }

    public static List<SelectItem> getScalarFunctionSelectItems(Iterable<SelectItem> selectItems) {
        return CollectionUtils.filter(selectItems, new Predicate<SelectItem>() {
            @Override
            public Boolean eval(SelectItem arg) {
                return arg.getScalarFunction() != null;
            }
        });
    }

    public static DataSet getOrdered(DataSet dataSet, List<OrderByItem> orderByItems) {
        return getOrdered(dataSet, orderByItems.toArray(new OrderByItem[orderByItems.size()]));
    }

    public static DataSet getOrdered(DataSet dataSet, final OrderByItem... orderByItems) {
        if (orderByItems != null && orderByItems.length != 0) {
            final int[] sortIndexes = new int[orderByItems.length];
            for (int i = 0; i < orderByItems.length; i++) {
                OrderByItem item = orderByItems[i];
                int indexOf = dataSet.indexOf(item.getSelectItem());
                sortIndexes[i] = indexOf;
            }

            final List<Row> data = readDataSetFull(dataSet);
            if (data.isEmpty()) {
                return new EmptyDataSet(dataSet.getSelectItems());
            }

            final Comparator<Object> valueComparator = ObjectComparator.getComparator();

            // create a comparator for doing the actual sorting/ordering
            final Comparator<Row> comparator = new Comparator<Row>() {
                public int compare(Row o1, Row o2) {
                    for (int i = 0; i < sortIndexes.length; i++) {
                        int sortIndex = sortIndexes[i];
                        Object sortObj1 = o1.getValue(sortIndex);
                        Object sortObj2 = o2.getValue(sortIndex);
                        int compare = valueComparator.compare(sortObj1, sortObj2);
                        if (compare != 0) {
                            OrderByItem orderByItem = orderByItems[i];
                            boolean ascending = orderByItem.isAscending();
                            if (ascending) {
                                return compare;
                            } else {
                                return compare * -1;
                            }
                        }
                    }
                    return 0;
                }
            };

            Collections.sort(data, comparator);

            dataSet = new InMemoryDataSet(data);
        }
        return dataSet;
    }

    public static List<Row> readDataSetFull(DataSet dataSet) {
        final List<Row> result;
        if (dataSet instanceof InMemoryDataSet) {
            // if dataset is an in memory dataset we have a shortcut to avoid
            // creating a new list
            result = ((InMemoryDataSet) dataSet).getRows();
        } else {
            result = new ArrayList<Row>();
            while (dataSet.next()) {
                result.add(dataSet.getRow());
            }
        }
        dataSet.close();
        return result;
    }

    /**
     * Examines a query and extracts an array of FromItem's that refer
     * (directly) to tables (hence Joined FromItems and SubQuery FromItems are
     * traversed but not included).
     * 
     * @param q
     *            the query to examine
     * @return an array of FromItem's that refer directly to tables
     */
    public static FromItem[] getTableFromItems(Query q) {
        List<FromItem> result = new ArrayList<FromItem>();
        List<FromItem> items = q.getFromClause().getItems();
        for (FromItem item : items) {
            result.addAll(getTableFromItems(item));
        }
        return result.toArray(new FromItem[result.size()]);
    }

    public static List<FromItem> getTableFromItems(FromItem item) {
        List<FromItem> result = new ArrayList<FromItem>();
        if (item.getTable() != null) {
            result.add(item);
        } else if (item.getSubQuery() != null) {
            FromItem[] sqItems = getTableFromItems(item.getSubQuery());
            for (int i = 0; i < sqItems.length; i++) {
                result.add(sqItems[i]);
            }
        } else if (item.getJoin() != null) {
            FromItem leftSide = item.getLeftSide();
            result.addAll(getTableFromItems(leftSide));
            FromItem rightSide = item.getRightSide();
            result.addAll(getTableFromItems(rightSide));
        } else {
            throw new IllegalStateException("FromItem was neither of Table type, SubQuery type or Join type: " + item);
        }
        return result;
    }

    /**
     * Executes a single row query, like "SELECT COUNT(*), MAX(SOME_COLUMN) FROM
     * MY_TABLE" or similar.
     * 
     * @param dataContext
     *            the DataContext object to use for executing the query
     * @param query
     *            the query to execute
     * @return a row object representing the single row returned from the query
     * @throws MetaModelException
     *             if less or more than one Row is returned from the query
     */
    public static Row executeSingleRowQuery(DataContext dataContext, Query query) throws MetaModelException {
        DataSet dataSet = dataContext.executeQuery(query);
        boolean next = dataSet.next();
        if (!next) {
            throw new MetaModelException("No rows returned from query: " + query);
        }
        Row row = dataSet.getRow();
        next = dataSet.next();
        if (next) {
            throw new MetaModelException("More than one row returned from query: " + query);
        }
        dataSet.close();
        return row;
    }

    /**
     * Performs a left join (aka left outer join) operation on two datasets.
     * 
     * @param ds1
     *            the left dataset
     * @param ds2
     *            the right dataset
     * @param onConditions
     *            the conditions to join by
     * @return the left joined result dataset
     */
    public static DataSet getLeftJoin(DataSet ds1, DataSet ds2, FilterItem[] onConditions) {
        if (ds1 == null) {
            throw new IllegalArgumentException("Left DataSet cannot be null");
        }
        if (ds2 == null) {
            throw new IllegalArgumentException("Right DataSet cannot be null");
        }
        SelectItem[] si1 = ds1.getSelectItems();
        SelectItem[] si2 = ds2.getSelectItems();
        SelectItem[] selectItems = new SelectItem[si1.length + si2.length];
        System.arraycopy(si1, 0, selectItems, 0, si1.length);
        System.arraycopy(si2, 0, selectItems, si1.length, si2.length);

        List<Row> resultRows = new ArrayList<Row>();
        List<Row> ds2data = readDataSetFull(ds2);
        if (ds2data.isEmpty()) {
            // no need to join, simply return a new view (with null values) on
            // the previous dataset.
            return getSelection(selectItems, ds1);
        }

        final DataSetHeader header = new CachingDataSetHeader(selectItems);

        while (ds1.next()) {

            // Construct a single-row dataset for making a carthesian product
            // against ds2
            Row ds1row = ds1.getRow();
            List<Row> ds1rows = new ArrayList<Row>();
            ds1rows.add(ds1row);

            DataSet carthesianProduct = getCarthesianProduct(
                    new DataSet[] { new InMemoryDataSet(new CachingDataSetHeader(si1), ds1rows),
                            new InMemoryDataSet(new CachingDataSetHeader(si2), ds2data) },
                    onConditions);
            List<Row> carthesianRows = readDataSetFull(carthesianProduct);
            if (carthesianRows.size() > 0) {
                resultRows.addAll(carthesianRows);
            } else {
                Object[] values = ds1row.getValues();
                Object[] row = new Object[selectItems.length];
                System.arraycopy(values, 0, row, 0, values.length);
                resultRows.add(new DefaultRow(header, row));
            }
        }
        ds1.close();

        if (resultRows.isEmpty()) {
            return new EmptyDataSet(selectItems);
        }

        return new InMemoryDataSet(header, resultRows);
    }

    /**
     * Performs a right join (aka right outer join) operation on two datasets.
     * 
     * @param ds1
     *            the left dataset
     * @param ds2
     *            the right dataset
     * @param onConditions
     *            the conditions to join by
     * @return the right joined result dataset
     */
    public static DataSet getRightJoin(DataSet ds1, DataSet ds2, FilterItem[] onConditions) {
        SelectItem[] ds1selects = ds1.getSelectItems();
        SelectItem[] ds2selects = ds2.getSelectItems();
        SelectItem[] leftOrderedSelects = new SelectItem[ds1selects.length + ds2selects.length];
        System.arraycopy(ds1selects, 0, leftOrderedSelects, 0, ds1selects.length);
        System.arraycopy(ds2selects, 0, leftOrderedSelects, ds1selects.length, ds2selects.length);

        // We will reuse the left join algorithm (but switch the datasets
        // around)
        DataSet dataSet = getLeftJoin(ds2, ds1, onConditions);

        dataSet = getSelection(leftOrderedSelects, dataSet);
        return dataSet;
    }

    public static SelectItem[] createSelectItems(Column... columns) {
        SelectItem[] items = new SelectItem[columns.length];
        for (int i = 0; i < items.length; i++) {
            items[i] = new SelectItem(columns[i]);
        }
        return items;
    }

    public static DataSet getDistinct(DataSet dataSet) {
        SelectItem[] selectItems = dataSet.getSelectItems();
        GroupByItem[] groupByItems = new GroupByItem[selectItems.length];
        for (int i = 0; i < groupByItems.length; i++) {
            groupByItems[i] = new GroupByItem(selectItems[i]);
        }
        return getGrouped(Arrays.asList(selectItems), dataSet, groupByItems);
    }

    public static Table[] getTables(Column[] columns) {
        return getTables(Arrays.asList(columns));
    }

    public static Column[] getColumnsByType(Column[] columns, final ColumnType columnType) {
        return CollectionUtils.filter(columns, new Predicate<Column>() {
            @Override
            public Boolean eval(Column column) {
                return column.getType() == columnType;
            }
        }).toArray(new Column[0]);
    }

    public static Column[] getColumnsBySuperType(Column[] columns, final SuperColumnType superColumnType) {
        return CollectionUtils.filter(columns, new Predicate<Column>() {
            @Override
            public Boolean eval(Column column) {
                return column.getType().getSuperType() == superColumnType;
            }
        }).toArray(new Column[0]);
    }

    public static Query parseQuery(DataContext dc, String queryString) {
        final QueryParser parser = new QueryParser(dc, queryString);
        return parser.parse();
    }

    public static DataSet getPaged(DataSet dataSet, int firstRow, int maxRows) {
        if (firstRow > 1) {
            dataSet = new FirstRowDataSet(dataSet, firstRow);
        }
        if (maxRows != -1) {
            dataSet = new MaxRowsDataSet(dataSet, maxRows);
        }
        return dataSet;
    }

    public static List<SelectItem> getEvaluatedSelectItems(final List<FilterItem> items) {
        final List<SelectItem> result = new ArrayList<SelectItem>();
        for (FilterItem item : items) {
            addEvaluatedSelectItems(result, item);
        }
        return result;
    }

    private static void addEvaluatedSelectItems(List<SelectItem> result, FilterItem item) {
        final FilterItem[] orItems = item.getChildItems();
        if (orItems != null) {
            for (FilterItem filterItem : orItems) {
                addEvaluatedSelectItems(result, filterItem);
            }
        }
        final SelectItem selectItem = item.getSelectItem();
        if (selectItem != null && !result.contains(selectItem)) {
            result.add(selectItem);
        }
        final Object operand = item.getOperand();
        if (operand != null && operand instanceof SelectItem && !result.contains(operand)) {
            result.add((SelectItem) operand);
        }
    }

    /**
     * This method returns the select item of the given alias name.
     * 
     * @param query
     * @return
     */
    public static SelectItem getSelectItemByAlias(Query query, String alias) {
        List<SelectItem> selectItems = query.getSelectClause().getItems();
        for (SelectItem selectItem : selectItems) {
            if (selectItem.getAlias() != null && selectItem.getAlias().equals(alias)) {
                return selectItem;
            }
        }
        return null;
    }

    /**
     * Determines if a query contains {@link ScalarFunction}s in any clause of
     * the query EXCEPT for the SELECT clause. This is a handy thing to
     * determine because decorating with {@link ScalarFunctionDataSet} only
     * gives you select-item evaluation so if the rest of the query is pushed to
     * an underlying datastore, then it may create issues.
     * 
     * @param query
     * @return
     */
    public static boolean containsNonSelectScalaFunctions(Query query) {
        // check FROM clause
        final List<FromItem> fromItems = query.getFromClause().getItems();
        for (FromItem fromItem : fromItems) {
            // check sub-queries
            final Query subQuery = fromItem.getSubQuery();
            if (subQuery != null) {
                if (containsNonSelectScalaFunctions(subQuery)) {
                    return true;
                }
                if (!getScalarFunctionSelectItems(subQuery.getSelectClause().getItems()).isEmpty()) {
                    return true;
                }
            }
        }

        // check WHERE clause
        if (!getScalarFunctionSelectItems(query.getWhereClause().getEvaluatedSelectItems()).isEmpty()) {
            return true;
        }

        // check GROUP BY clause
        if (!getScalarFunctionSelectItems(query.getGroupByClause().getEvaluatedSelectItems()).isEmpty()) {
            return true;
        }

        // check HAVING clause
        if (!getScalarFunctionSelectItems(query.getHavingClause().getEvaluatedSelectItems()).isEmpty()) {
            return true;
        }

        // check ORDER BY clause
        if (!getScalarFunctionSelectItems(query.getOrderByClause().getEvaluatedSelectItems()).isEmpty()) {
            return true;
        }

        return false;
    }
}