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

import java.util.HashMap;
import java.util.Map;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.insert.AbstractRowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

final class DynamoDbRowInsertionBuilder extends AbstractRowInsertionBuilder<DynamoDbUpdateCallback> {

    public DynamoDbRowInsertionBuilder(DynamoDbUpdateCallback updateCallback, Table table) {
        super(updateCallback, table);
    }

    @Override
    public void execute() throws MetaModelException {
        final Map<String, AttributeValue> itemValues = new HashMap<>();
        final Column[] columns = getColumns();
        final Object[] values = getValues();
        for (int i = 0; i < columns.length; i++) {
            final Column column = columns[i];
            final Object value = values[i];
            if (column.isPrimaryKey() && value == null) {
                throw new IllegalArgumentException("Value for '" + column.getName() + "' cannot be null");
            }

            final AttributeValue attributeValue = DynamoDbUtils.toAttributeValue(value);
            itemValues.put(column.getName(), attributeValue);
        }

        final AmazonDynamoDB dynamoDb = getUpdateCallback().getDataContext().getDynamoDb();
        dynamoDb.putItem(getTable().getName(), itemValues);
    }
}
