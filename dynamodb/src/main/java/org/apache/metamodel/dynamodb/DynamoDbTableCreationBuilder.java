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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.create.AbstractTableCreationBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

class DynamoDbTableCreationBuilder extends AbstractTableCreationBuilder<DynamoDbUpdateCallback> {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDbTableCreationBuilder.class);

    public DynamoDbTableCreationBuilder(DynamoDbUpdateCallback updateCallback, Schema schema, String name) {
        super(updateCallback, schema, name);
    }

    @Override
    public Table execute() throws MetaModelException {
        final MutableTable table = getTable();
        final String tableName = table.getName();

        final Collection<AttributeDefinition> attributes = new ArrayList<>();
        final Collection<KeySchemaElement> keySchema = new ArrayList<>();
        final Collection<GlobalSecondaryIndex> globalSecondaryIndices = new ArrayList<>();

        final long readCapacity = Long.parseLong(System.getProperty(
                DynamoDbDataContext.SYSTEM_PROPERTY_THROUGHPUT_READ_CAPACITY, "5"));
        final long writeCapacity = Long.parseLong(System.getProperty(
                DynamoDbDataContext.SYSTEM_PROPERTY_THROUGHPUT_WRITE_CAPACITY, "5"));
        final ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput(readCapacity, writeCapacity);

        final Column[] columns = table.getColumns();
        for (Column column : columns) {
            if (column.isPrimaryKey()) {
                final KeyType keyType = getKeyType(column.getRemarks());
                keySchema.add(new KeySchemaElement(column.getName(), keyType));
                attributes.add(new AttributeDefinition(column.getName(), DynamoDbUtils.toAttributeType(column
                        .getType())));
            }
        }

        final CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setTableName(tableName);
        createTableRequest.setAttributeDefinitions(attributes);
        createTableRequest.setGlobalSecondaryIndexes(globalSecondaryIndices);
        createTableRequest.setKeySchema(keySchema);
        createTableRequest.setProvisionedThroughput(provisionedThroughput);

        final AmazonDynamoDB client = getUpdateCallback().getDataContext().getDynamoDb();

        final CreateTableResult createTableResult = client.createTable(createTableRequest);

        // await the table creation to be "done".
        {
            String tableStatus = createTableResult.getTableDescription().getTableStatus();
            while (TableStatus.CREATING.name().equals(tableStatus)) {
                logger.debug("Waiting for table status to be ACTIVE. Currently: {}", tableStatus);
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    getUpdateCallback().setInterrupted(true);
                }
                tableStatus = client.describeTable(tableName).getTable().getTableStatus();
            }
        }

        return table;
    }

    private KeyType getKeyType(String remarks) {
        if ("RANGE".equals(remarks)) {
            // default
            return KeyType.RANGE;
        }
        return KeyType.HASH;
    }

}
