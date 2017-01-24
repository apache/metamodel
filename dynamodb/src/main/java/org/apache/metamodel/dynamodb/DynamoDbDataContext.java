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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
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
public class DynamoDbDataContext extends QueryPostprocessDataContext implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDbDataContext.class);

    private static final String SCHEMA_NAME = "public";

    private final AmazonDynamoDB _client;
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
        _client = client;
        _tableDefs = (tableDefs == null ? new SimpleTableDef[0] : tableDefs);
        _shutdownOnClose = shutdownOnClose;
    }

    @Override
    public void close() {
        if (_shutdownOnClose) {
            _client.shutdown();
        }
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final Map<String, SimpleTableDef> tableDefs = new HashMap<>();
        for (SimpleTableDef tableDef : _tableDefs) {
            tableDefs.put(tableDef.getName(), tableDef);
        }

        final MutableSchema schema = new MutableSchema(getMainSchemaName());
        final ListTablesResult tables = _client.listTables();
        final List<String> tableNames = tables.getTableNames();
        for (String tableName : tableNames) {
            final MutableTable table = new MutableTable(tableName, schema);
            schema.addTable(table);

            final DescribeTableResult descripeTableResult = _client.describeTable(tableName);
            final TableDescription tableDescription = descripeTableResult.getTable();

            // add primary keys
            addColumnFromKeySchema("Primary index", tableDescription.getKeySchema(), table, true);

            // add attributes from global and local indices
            final List<GlobalSecondaryIndexDescription> globalSecondaryIndexes = tableDescription
                    .getGlobalSecondaryIndexes();
            if (globalSecondaryIndexes != null) {
                for (GlobalSecondaryIndexDescription globalSecondaryIndex : globalSecondaryIndexes) {
                    addColumnFromKeySchema(globalSecondaryIndex.getIndexName(), globalSecondaryIndex.getKeySchema(),
                            table, false);
                }
            }
            final List<LocalSecondaryIndexDescription> localSecondaryIndexes = tableDescription
                    .getLocalSecondaryIndexes();
            if (localSecondaryIndexes != null) {
                for (LocalSecondaryIndexDescription localSecondaryIndex : localSecondaryIndexes) {
                    addColumnFromKeySchema(localSecondaryIndex.getIndexName(), localSecondaryIndex.getKeySchema(),
                            table, false);
                }
            }

            // add top-level attribute definitions
            final List<AttributeDefinition> attributeDefinitions = tableDescription.getAttributeDefinitions();
            for (AttributeDefinition attributeDefinition : attributeDefinitions) {
                final String attributeName = attributeDefinition.getAttributeName();
                MutableColumn column = (MutableColumn) table.getColumnByName(attributeName);
                if (column == null) {
                    column = new MutableColumn(attributeName, table);
                    table.addColumn(column);
                }
                final String attributeType = attributeDefinition.getAttributeType();
                column.setType(toColumnType(attributeName, attributeType));
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
                for (GlobalSecondaryIndexDescription globalSecondaryIndex : globalSecondaryIndexes) {
                    final List<String> nonKeyAttributes = globalSecondaryIndex.getProjection().getNonKeyAttributes();
                    for (String attributeName : nonKeyAttributes) {
                        addColumnFromNonKeyAttribute(globalSecondaryIndex.getIndexName(), table, attributeName);
                    }
                }
            }
            if (localSecondaryIndexes != null) {
                for (LocalSecondaryIndexDescription localSecondaryIndex : localSecondaryIndexes) {
                    final List<String> nonKeyAttributes = localSecondaryIndex.getProjection().getNonKeyAttributes();
                    for (String attributeName : nonKeyAttributes) {
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
        for (KeySchemaElement keySchemaElement : keySchema) {
            final String attributeName = keySchemaElement.getAttributeName();
            if (table.getColumnByName(attributeName) == null) {
                final String keyType = keySchemaElement.getKeyType();
                final MutableColumn column = new MutableColumn(attributeName, table).setPrimaryKey(primaryKey);
                appendRemarks(column, indexName + " member ('" + keyType + "' type)");
                table.addColumn(column);
            }
        }
    }

    private void appendRemarks(MutableColumn column, String remarks) {
        final String existingRemarks = column.getRemarks();
        if (existingRemarks == null) {
            column.setRemarks(remarks);
        } else {
            column.setRemarks(existingRemarks + ", " + remarks);
        }
    }

    private ColumnType toColumnType(String attributeName, String attributeType) {
        if (attributeType == null) {
            return null;
        }
        switch (attributeType) {
        case "S":
            return ColumnType.STRING;
        case "N":
            return ColumnType.NUMBER;
        case "B":
            return ColumnType.BINARY;
        }
        logger.warn("Unexpected attribute type '{}' for attribute: {}", attributeType, attributeName);
        return null;
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
        return _client.describeTable(table.getName()).getTable().getItemCount();
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        List<String> attributeNames = new ArrayList<>(columns.length);
        for (Column column : columns) {
            attributeNames.add(column.getName());
        }
        ScanRequest scanRequest = new ScanRequest(table.getName());
        scanRequest.setAttributesToGet(attributeNames);
        if (maxRows > 0) {
            scanRequest.setLimit(maxRows);
        }
        final ScanResult result = _client.scan(scanRequest);
        return new DynamoDbDataSet(columns, result);
    }
}
