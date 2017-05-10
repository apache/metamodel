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
package org.apache.metamodel.dynamodb;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateSummary;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

/**
 * DataContext implementation for Amazon DynamoDB.
 */
public class DynamoDbDataContext extends QueryPostprocessDataContext implements UpdateableDataContext, Closeable {

    /**
     * System property key used for getting the read throughput capacity when
     * creating new tables. Defaults to 5.
     */
    public static final String SYSTEM_PROPERTY_THROUGHPUT_READ_CAPACITY = "metamodel.dynamodb.throughput.capacity.read";

    /**
     * System property key used for getting the write throughput capacity when
     * creating new tables. Defaults to 5.
     */
    public static final String SYSTEM_PROPERTY_THROUGHPUT_WRITE_CAPACITY = "metamodel.dynamodb.throughput.capacity.write";

    /**
     * The artificial schema name used by this DataContext.
     */
    public static final String SCHEMA_NAME = "public";

    private final AmazonDynamoDB _dynamoDb;
    private final boolean _shutdownOnClose;
    private final SimpleTableDef[] _tableDefs;

    public DynamoDbDataContext() {
        this(AmazonDynamoDBClientBuilder.defaultClient(), null, true);
    }

    public DynamoDbDataContext(SimpleTableDef[] tableDefs) {
        this(AmazonDynamoDBClientBuilder.defaultClient(), tableDefs, true);
    }

    public DynamoDbDataContext(AmazonDynamoDB client) {
        this(client, null, false);
    }

    public DynamoDbDataContext(AmazonDynamoDB client, SimpleTableDef[] tableDefs) {
        this(client, tableDefs, false);
    }

    private DynamoDbDataContext(AmazonDynamoDB client, SimpleTableDef[] tableDefs, boolean shutdownOnClose) {
        _dynamoDb = client;
        _tableDefs = (tableDefs == null ? new SimpleTableDef[0] : tableDefs);
        _shutdownOnClose = shutdownOnClose;
    }

    public AmazonDynamoDB getDynamoDb() {
        return _dynamoDb;
    }

    @Override
    public void close() {
        if (_shutdownOnClose) {
            _dynamoDb.shutdown();
        }
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final Map<String, SimpleTableDef> tableDefs = new HashMap<>();
        for (final SimpleTableDef tableDef : _tableDefs) {
            tableDefs.put(tableDef.getName(), tableDef);
        }

        final MutableSchema schema = new MutableSchema(getMainSchemaName());
        final ListTablesResult tables = _dynamoDb.listTables();
        final List<String> tableNames = tables.getTableNames();
        for (final String tableName : tableNames) {
            final MutableTable table = new MutableTable(tableName, schema);
            schema.addTable(table);

            final DescribeTableResult descripeTableResult = _dynamoDb.describeTable(tableName);
            final TableDescription tableDescription = descripeTableResult.getTable();

            // add primary keys
            addColumnFromKeySchema("Primary index", tableDescription.getKeySchema(), table, true);

            // add attributes from global and local indices
            final List<GlobalSecondaryIndexDescription> globalSecondaryIndexes = tableDescription
                    .getGlobalSecondaryIndexes();
            if (globalSecondaryIndexes != null) {
                for (final GlobalSecondaryIndexDescription globalSecondaryIndex : globalSecondaryIndexes) {
                    addColumnFromKeySchema(globalSecondaryIndex.getIndexName(), globalSecondaryIndex.getKeySchema(),
                            table, false);
                }
            }
            final List<LocalSecondaryIndexDescription> localSecondaryIndexes = tableDescription
                    .getLocalSecondaryIndexes();
            if (localSecondaryIndexes != null) {
                for (final LocalSecondaryIndexDescription localSecondaryIndex : localSecondaryIndexes) {
                    addColumnFromKeySchema(localSecondaryIndex.getIndexName(), localSecondaryIndex.getKeySchema(),
                            table, false);
                }
            }

            // add top-level attribute definitions
            final List<AttributeDefinition> attributeDefinitions = tableDescription.getAttributeDefinitions();
            for (final AttributeDefinition attributeDefinition : attributeDefinitions) {
                final String attributeName = attributeDefinition.getAttributeName();
                MutableColumn column = (MutableColumn) table.getColumnByName(attributeName);
                if (column == null) {
                    column = new MutableColumn(attributeName, table);
                    table.addColumn(column);
                }
                final String attributeType = attributeDefinition.getAttributeType();
                column.setType(DynamoDbUtils.toColumnType(attributeName, attributeType));
                column.setIndexed(true);
                column.setNativeType(attributeType);
            }

            // add additional metadata from SimpleTableDefs if available
            final SimpleTableDef tableDef = tableDefs.get(tableName);
            if (tableDef != null) {
                final String[] columnNames = tableDef.getColumnNames();
                final ColumnType[] columnTypes = tableDef.getColumnTypes();
                for (int i = 0; i < columnNames.length; i++) {
                    final String columnName = columnNames[i];
                    final ColumnType columnType = columnTypes[i];
                    MutableColumn column = (MutableColumn) table.getColumnByName(columnName);
                    if (column == null) {
                        column = new MutableColumn(columnName, table);
                        table.addColumn(column);
                    }
                    if (column.getType() == null && columnType != null) {
                        column.setType(columnType);
                    }
                }
            }

            // add additional attributes based on global and local indices
            if (globalSecondaryIndexes != null) {
                for (final GlobalSecondaryIndexDescription globalSecondaryIndex : globalSecondaryIndexes) {
                    final List<String> nonKeyAttributes = globalSecondaryIndex.getProjection().getNonKeyAttributes();
                    for (final String attributeName : nonKeyAttributes) {
                        addColumnFromNonKeyAttribute(globalSecondaryIndex.getIndexName(), table, attributeName);
                    }
                }
            }
            if (localSecondaryIndexes != null) {
                for (final LocalSecondaryIndexDescription localSecondaryIndex : localSecondaryIndexes) {
                    final List<String> nonKeyAttributes = localSecondaryIndex.getProjection().getNonKeyAttributes();
                    for (final String attributeName : nonKeyAttributes) {
                        addColumnFromNonKeyAttribute(localSecondaryIndex.getIndexName(), table, attributeName);
                    }
                }
            }
        }
        return schema;
    }

    private void addColumnFromNonKeyAttribute(String indexName, MutableTable table, String attributeName) {
        MutableColumn column = (MutableColumn) table.getColumnByName(attributeName);
        if (column == null) {
            column = new MutableColumn(attributeName, table);
            table.addColumn(column);
        }
        appendRemarks(column, indexName + " non-key attribute");
    }

    private void addColumnFromKeySchema(String indexName, List<KeySchemaElement> keySchema, MutableTable table,
            boolean primaryKey) {
        for (final KeySchemaElement keySchemaElement : keySchema) {
            final String attributeName = keySchemaElement.getAttributeName();
            if (table.getColumnByName(attributeName) == null) {
                final String keyType = keySchemaElement.getKeyType();
                final MutableColumn column = new MutableColumn(attributeName, table).setPrimaryKey(primaryKey);
                appendRemarks(column, indexName + " member ('" + keyType + "' type)");
                table.addColumn(column);
            }
        }
    }

    private static void appendRemarks(MutableColumn column, String remarks) {
        final String existingRemarks = column.getRemarks();
        if (existingRemarks == null) {
            column.setRemarks(remarks);
        } else {
            column.setRemarks(existingRemarks + ", " + remarks);
        }
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return SCHEMA_NAME;
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        if (!whereItems.isEmpty()) {
            return null;
        }
        return _dynamoDb.describeTable(table.getName()).getTable().getItemCount();
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        final List<String> attributeNames = new ArrayList<>(columns.length);
        for (final Column column : columns) {
            attributeNames.add(column.getName());
        }
        final ScanRequest scanRequest = new ScanRequest(table.getName());
        scanRequest.setAttributesToGet(attributeNames);
        if (maxRows > 0) {
            scanRequest.setLimit(maxRows);
        }
        final ScanResult result = _dynamoDb.scan(scanRequest);
        return new DynamoDbDataSet(columns, result);
    }

    @Override
    protected Row executePrimaryKeyLookupQuery(Table table, List<SelectItem> selectItems, Column primaryKeyColumn,
            Object keyValue) {
        final List<String> attributeNames = new ArrayList<>();
        for (SelectItem selectItem : selectItems) {
            attributeNames.add(selectItem.getColumn().getName());
        }

        final GetItemRequest getItemRequest = new GetItemRequest(table.getName(), Collections.singletonMap(
                primaryKeyColumn.getName(), DynamoDbUtils.toAttributeValue(keyValue))).withAttributesToGet(
                        attributeNames);
        final GetItemResult item = _dynamoDb.getItem(getItemRequest);

        final Object[] values = new Object[selectItems.size()];
        for (int i = 0; i < values.length; i++) {
            final AttributeValue attributeValue = item.getItem().get(attributeNames.get(i));
            values[i] = DynamoDbUtils.toValue(attributeValue);
        }

        return new DefaultRow(new SimpleDataSetHeader(selectItems), values);
    }

    @Override
    public UpdateSummary executeUpdate(UpdateScript update) {
        final DynamoDbUpdateCallback callback = new DynamoDbUpdateCallback(this);
        try {
            update.run(callback);
        } finally {
            if (callback.isInterrupted()) {
                Thread.currentThread().interrupt();
            }
        }
        return callback.getUpdateSummary();
    }
}
