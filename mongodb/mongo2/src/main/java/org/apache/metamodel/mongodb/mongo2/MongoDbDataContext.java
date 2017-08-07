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
package org.apache.metamodel.mongodb.mongo2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateSummary;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.InMemoryDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.mongodb.common.MongoDBUtils;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ColumnTypeImpl;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

/**
 * DataContext implementation for MongoDB.
 *
 * Since MongoDB has no schema, a virtual schema will be used in this
 * DataContext. This implementation supports either automatic discovery of a
 * schema or manual specification of a schema, through the
 * {@link MongoDbTableDef} class.
 */
public class MongoDbDataContext extends QueryPostprocessDataContext implements UpdateableDataContext {

    private static final Logger logger = LoggerFactory.getLogger(MongoDbDataSet.class);

    private final DB _mongoDb;
    private final SimpleTableDef[] _tableDefs;
    private WriteConcernAdvisor _writeConcernAdvisor;
    private Schema _schema;

    /**
     * Constructs a {@link MongoDbDataContext}. This constructor accepts a
     * custom array of {@link MongoDbTableDef}s which allows the user to define
     * his own view on the collections in the database.
     *
     * @param mongoDb
     *            the mongo db connection
     * @param tableDefs
     *            an array of {@link MongoDbTableDef}s, which define the table
     *            and column model of the mongo db collections. (consider using
     *            {@link #detectSchema(DB)} or {@link #detectTable(DB, String)}
     *            ).
     */
    public MongoDbDataContext(DB mongoDb, SimpleTableDef... tableDefs) {
        _mongoDb = mongoDb;
        _tableDefs = tableDefs;
        _schema = null;
    }

    /**
     * Constructs a {@link MongoDbDataContext} and automatically detects the
     * schema structure/view on all collections (see {@link #detectSchema(DB)}).
     *
     * @param mongoDb
     *            the mongo db connection
     */
    public MongoDbDataContext(DB mongoDb) {
        this(mongoDb, detectSchema(mongoDb));
    }

    /**
     * Performs an analysis of the available collections in a Mongo {@link DB}
     * instance and tries to detect the table's structure based on the first
     * 1000 documents in each collection.
     *
     * @param db
     *            the mongo db to inspect
     * @return a mutable schema instance, useful for further fine tuning by the
     *         user.
     * @see #detectTable(DB, String)
     */
    public static SimpleTableDef[] detectSchema(DB db) {
        Set<String> collectionNames = db.getCollectionNames();
        SimpleTableDef[] result = new SimpleTableDef[collectionNames.size()];
        int i = 0;
        for (String collectionName : collectionNames) {
            SimpleTableDef table = detectTable(db, collectionName);
            result[i] = table;
            i++;
        }
        return result;
    }

    /**
     * Performs an analysis of an available collection in a Mongo {@link DB}
     * instance and tries to detect the table structure based on the first 1000
     * documents in the collection.
     *
     * @param db
     *            the mongo DB
     * @param collectionName
     *            the name of the collection
     * @return a table definition for mongo db.
     */
    public static SimpleTableDef detectTable(DB db, String collectionName) {
        final DBCollection collection = db.getCollection(collectionName);
        final DBCursor cursor = collection.find().limit(1000);

        final SortedMap<String, Set<Class<?>>> columnsAndTypes = new TreeMap<String, Set<Class<?>>>();
        while (cursor.hasNext()) {
            DBObject object = cursor.next();
            Set<String> keysInObject = object.keySet();
            for (String key : keysInObject) {
                Set<Class<?>> types = columnsAndTypes.get(key);
                if (types == null) {
                    types = new HashSet<Class<?>>();
                    columnsAndTypes.put(key, types);
                }
                Object value = object.get(key);
                if (value != null) {
                    types.add(value.getClass());
                }
            }
        }
        cursor.close();

        final String[] columnNames = new String[columnsAndTypes.size()];
        final ColumnType[] columnTypes = new ColumnType[columnsAndTypes.size()];

        int i = 0;
        for (Entry<String, Set<Class<?>>> columnAndTypes : columnsAndTypes.entrySet()) {
            final String columnName = columnAndTypes.getKey();
            final Set<Class<?>> columnTypeSet = columnAndTypes.getValue();
            final Class<?> columnType;
            if (columnTypeSet.size() == 1) {
                columnType = columnTypeSet.iterator().next();
            } else {
                columnType = Object.class;
            }
            columnNames[i] = columnName;
            if (columnType == ObjectId.class) {
                columnTypes[i] = ColumnType.ROWID;
            } else {
                columnTypes[i] = ColumnTypeImpl.convertColumnType(columnType);
            }
            i++;
        }

        return new SimpleTableDef(collectionName, columnNames, columnTypes);
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        if (_schema == null) {
            MutableSchema schema = new MutableSchema(getMainSchemaName());
            for (SimpleTableDef tableDef : _tableDefs) {

                MutableTable table = tableDef.toTable().setSchema(schema);
                for (Column column : table.getColumnsOfType(ColumnType.ROWID)) {
                    if (column instanceof MutableColumn) {
                        ((MutableColumn) column).setPrimaryKey(true);
                    }
                }

                schema.addTable(table);
            }

            _schema = schema;
        }
        return _schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _mongoDb.getName();
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        final DBCollection collection = _mongoDb.getCollection(table.getName());

        final DBObject query = createMongoDbQuery(table, whereItems);

        logger.info("Executing MongoDB 'count' query: {}", query);
        final long count = collection.count(query);

        return count;
    }

    @Override
    protected Row executePrimaryKeyLookupQuery(Table table, List<SelectItem> selectItems, Column primaryKeyColumn,
            Object keyValue) {
        final DBCollection collection = _mongoDb.getCollection(table.getName());

        List<FilterItem> whereItems = new ArrayList<FilterItem>();
        SelectItem selectItem = new SelectItem(primaryKeyColumn);
        FilterItem primaryKeyWhereItem = new FilterItem(selectItem, OperatorType.EQUALS_TO, keyValue);
        whereItems.add(primaryKeyWhereItem);
        final DBObject query = createMongoDbQuery(table, whereItems);
        final DBObject resultDBObject = collection.findOne(query);

        DataSetHeader header = new SimpleDataSetHeader(selectItems);

        Row row = MongoDBUtils.toRow(resultDBObject, header);

        return row;
    }

    @Override
    public DataSet executeQuery(Query query) {
        // Check for queries containing only simple selects and where clauses,
        // or if it is a COUNT(*) query.

        // if from clause only contains a main schema table
        List<FromItem> fromItems = query.getFromClause().getItems();
        if (fromItems.size() == 1 && fromItems.get(0).getTable() != null
                && fromItems.get(0).getTable().getSchema() == _schema) {
            final Table table = fromItems.get(0).getTable();

            // if GROUP BY, HAVING and ORDER BY clauses are not specified
            if (query.getGroupByClause().isEmpty() && query.getHavingClause().isEmpty()
                    && query.getOrderByClause().isEmpty()) {

                final List<FilterItem> whereItems = query.getWhereClause().getItems();

                // if all of the select items are "pure" column selection
                boolean allSelectItemsAreColumns = true;
                List<SelectItem> selectItems = query.getSelectClause().getItems();

                // if it is a
                // "SELECT [columns] FROM [table] WHERE [conditions]"
                // query.
                for (SelectItem selectItem : selectItems) {
                    if (selectItem.hasFunction() || selectItem.getColumn() == null) {
                        allSelectItemsAreColumns = false;
                        break;
                    }
                }

                if (allSelectItemsAreColumns) {
                    logger.debug("Query can be expressed in full MongoDB, no post processing needed.");

                    // prepare for a non-post-processed query


                    // checking if the query is a primary key lookup query
                    if (whereItems.size() == 1) {
                        final FilterItem whereItem = whereItems.get(0);
                        final SelectItem selectItem = whereItem.getSelectItem();
                        if (!whereItem.isCompoundFilter() && selectItem != null && selectItem.getColumn() != null) {
                            final Column column = selectItem.getColumn();
                            if (column.isPrimaryKey() && OperatorType.EQUALS_TO.equals(whereItem.getOperator())) {
                                logger.debug("Query is a primary key lookup query. Trying executePrimaryKeyLookupQuery(...)");
                                final Object operand = whereItem.getOperand();
                                final Row row = executePrimaryKeyLookupQuery(table, selectItems, column, operand);
                                if (row == null) {
                                    logger.debug("DataContext did not return any primary key lookup query results. Proceeding "
                                            + "with manual lookup.");
                                } else {
                                    final DataSetHeader header = new SimpleDataSetHeader(selectItems);
                                    return new InMemoryDataSet(header, row);
                                }
                            }
                        }
                    }

                    int firstRow = (query.getFirstRow() == null ? 1 : query.getFirstRow());
                    int maxRows = (query.getMaxRows() == null ? -1 : query.getMaxRows());
                    boolean thereIsAtLeastOneAlias = false;

                    for (SelectItem selectItem : selectItems) {
                        if (selectItem.getAlias() != null) {
                            thereIsAtLeastOneAlias = true;
                            break;
                        }
                    }

                    if (thereIsAtLeastOneAlias) {
                        final DataSet dataSet = materializeMainSchemaTableInternal(
                                table,
                                selectItems,
                                whereItems,
                                firstRow,
                                maxRows, false);
                        return dataSet;
                    } else {
                        final DataSet dataSet = materializeMainSchemaTableInternal(table, selectItems, whereItems, firstRow,
                                maxRows, false);
                        return dataSet;
                    }
                }
            }
        }

        logger.debug("Query will be simplified for MongoDB and post processed.");
        return super.executeQuery(query);
    }



    private DataSet materializeMainSchemaTableInternal(Table table, List<SelectItem> selectItems,
            List<FilterItem> whereItems, int firstRow, int maxRows, boolean queryPostProcessed) {
        DBCursor cursor = getCursor(table, whereItems, firstRow, maxRows);

        return new MongoDbDataSet(cursor, selectItems, queryPostProcessed);
    }

    private DBCursor getCursor(Table table, List<FilterItem> whereItems, int firstRow, int maxRows) {
        final DBCollection collection = _mongoDb.getCollection(table.getName());

        final DBObject query = createMongoDbQuery(table, whereItems);

        logger.info("Executing MongoDB 'find' query: {}", query);
        DBCursor cursor = collection.find(query);

        if (maxRows > 0) {
            cursor = cursor.limit(maxRows);
        }
        if (firstRow > 1) {
            final int skip = firstRow - 1;
            cursor = cursor.skip(skip);
        }
        return cursor;
    }

    protected BasicDBObject createMongoDbQuery(Table table, List<FilterItem> whereItems) {
        assert _schema == table.getSchema();

        final BasicDBObject query = new BasicDBObject();
        if (whereItems != null && !whereItems.isEmpty()) {
            for (FilterItem item : whereItems) {
                convertToCursorObject(query, item);
            }
        }

        return query;
    }

    private void convertToCursorObject(BasicDBObject query, FilterItem item) {
        if (item.isCompoundFilter()) {

            BasicDBList orList = new BasicDBList();

            final FilterItem[] childItems = item.getChildItems();
            for (FilterItem childItem : childItems) {
                BasicDBObject childObject = new BasicDBObject();
                convertToCursorObject(childObject, childItem);
                orList.add(childObject);
            }

            query.put("$or", orList);

        } else {

            final Column column = item.getSelectItem().getColumn();
            final String columnName = column.getName();
            final String operatorName = getOperatorName(item);

            Object operand = item.getOperand();
            if (ObjectId.isValid(String.valueOf(operand))) {
                operand = new ObjectId(String.valueOf(operand));
            }

            final BasicDBObject existingFilterObject = (BasicDBObject) query.get(columnName);
            if (existingFilterObject == null) {
                if (operatorName == null) {
                    if (OperatorType.LIKE.equals(item.getOperator())) {
                        query.put(columnName, turnOperandIntoRegExp(operand));
                    } else {
                        query.put(columnName, operand);
                    }
                } else {
                    query.put(columnName, new BasicDBObject(operatorName, operand));
                }
            } else {
                if (operatorName == null) {
                    throw new IllegalStateException("Cannot retrieve records for a column with two EQUALS_TO operators");
                } else {
                    existingFilterObject.append(operatorName, operand);
                }
            }
        }
    }

    private String getOperatorName(FilterItem item) {
        final OperatorType operator = item.getOperator();

        if (OperatorType.EQUALS_TO.equals(operator)) {
            return null;
        }
        if (OperatorType.LIKE.equals(operator)) {
            return null;
        }
        if (OperatorType.LESS_THAN.equals(operator)) {
            return "$lt";
        }
        if (OperatorType.LESS_THAN_OR_EQUAL.equals(operator)) {
            return "$lte";
        }
        if (OperatorType.GREATER_THAN.equals(operator)) {
            return "$gt";
        }
        if (OperatorType.GREATER_THAN_OR_EQUAL.equals(operator)) {
            return "$gte";
        }
        if (OperatorType.DIFFERENT_FROM.equals(operator)) {
            return "$ne";
        }
        if (OperatorType.IN.equals(operator)) {
            return "$in";
        }

        throw new IllegalStateException("Unsupported operator type: " + operator);
    }

    private Pattern turnOperandIntoRegExp(Object operand) {
        StringBuilder operandAsRegExp = new StringBuilder(replaceWildCardLikeChars(operand.toString()));
        operandAsRegExp.insert(0, "^").append("$");
        return Pattern.compile(operandAsRegExp.toString(), Pattern.CASE_INSENSITIVE);
    }

    private String replaceWildCardLikeChars(String operand) {
        return operand.replaceAll("%", ".*");
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
        return materializeMainSchemaTableInternal(
                table,
                columns.stream().map(SelectItem::new).collect(Collectors.toList()),
                null,
                1,
                maxRows,
                true);
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int firstRow, int maxRows) {
        return materializeMainSchemaTableInternal(
                table,
                columns.stream().map(SelectItem::new).collect(Collectors.toList()),
                null,
                firstRow,
                maxRows,
                true);
    }

    /**
     * Executes an update with a specific {@link WriteConcernAdvisor}.
     */
    public UpdateSummary executeUpdate(UpdateScript update, WriteConcernAdvisor writeConcernAdvisor) {
        MongoDbUpdateCallback callback = new MongoDbUpdateCallback(this, writeConcernAdvisor);
        try {
            update.run(callback);
        } finally {
            callback.close();
        }
        return callback.getUpdateSummary();
    }

    /**
     * Executes an update with a specific {@link WriteConcern}.
     */
    public UpdateSummary executeUpdate(UpdateScript update, WriteConcern writeConcern) {
        return executeUpdate(update, new SimpleWriteConcernAdvisor(writeConcern));
    }

    @Override
    public UpdateSummary executeUpdate(UpdateScript update) {
        return executeUpdate(update, getWriteConcernAdvisor());
    }

    /**
     * Gets the {@link WriteConcernAdvisor} to use on
     * {@link #executeUpdate(UpdateScript)} calls.
     */
    public WriteConcernAdvisor getWriteConcernAdvisor() {
        if (_writeConcernAdvisor == null) {
            return new DefaultWriteConcernAdvisor();
        }
        return _writeConcernAdvisor;
    }

    /**
     * Sets a global {@link WriteConcern} advisor to use on
     * {@link #executeUpdate(UpdateScript)}.
     */
    public void setWriteConcernAdvisor(WriteConcernAdvisor writeConcernAdvisor) {
        _writeConcernAdvisor = writeConcernAdvisor;
    }

    /**
     * Gets the {@link DB} instance that this {@link DataContext} is backed by.
     */
    public DB getMongoDb() {
        return _mongoDb;
    }

    protected void addTable(MutableTable table) {
        if (_schema instanceof MutableSchema) {
            MutableSchema mutableSchema = (MutableSchema) _schema;
            mutableSchema.addTable(table);
        } else {
            throw new UnsupportedOperationException("Schema is not mutable");
        }
    }
}
