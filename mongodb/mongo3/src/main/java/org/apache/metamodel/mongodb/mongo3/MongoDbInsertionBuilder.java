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
package org.apache.metamodel.mongodb.mongo3;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.insert.AbstractRowInsertionBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.bson.Document;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;

final class MongoDbInsertionBuilder extends AbstractRowInsertionBuilder<MongoDbUpdateCallback> implements RowInsertionBuilder {

    private static final Logger logger = LoggerFactory.getLogger(MongoDbInsertionBuilder.class);

    public MongoDbInsertionBuilder(MongoDbUpdateCallback updateCallback, Table table) {
        super(updateCallback, table);
    }

    @Override
    public void execute() throws MetaModelException {
        final Column[] columns = getColumns();
        final Object[] values = getValues();

        final Document doc = new Document();

        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value != null) {
                doc.put(columns[i].getName(), value);
            }
        }

        final MongoDbUpdateCallback updateCallback = getUpdateCallback();
        final MongoCollection<Document> collection = updateCallback.getCollection(getTable().getName());
        final WriteConcern writeConcern = updateCallback.getWriteConcernAdvisor().adviceInsert(collection, doc);
        collection.withWriteConcern(writeConcern);

        collection.insertOne(doc);
        logger.info("Document has been inserted");
    }
}
