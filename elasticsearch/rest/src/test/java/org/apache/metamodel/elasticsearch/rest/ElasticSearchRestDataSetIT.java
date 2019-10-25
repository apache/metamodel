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
package org.apache.metamodel.elasticsearch.rest;

import static org.apache.metamodel.elasticsearch.rest.ElasticSearchRestDataContext.DEFAULT_TABLE_NAME;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.stream.IntStream;

import org.apache.http.HttpHost;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.CreateTable;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ElasticSearchRestDataSetIT {
    private static final String INDEX_NAME = "persons";

    private RestHighLevelClient client;

    @Before
    public void setUp() throws Exception {
        final String dockerHostAddress = ElasticSearchRestDataContextIT.determineHostName();

        client = ElasticSearchRestUtil
                .createClient(new HttpHost(dockerHostAddress, ElasticSearchRestDataContextIT.DEFAULT_REST_CLIENT_PORT),
                        null, null);
        client.indices().create(new CreateIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);
    }

    @After
    public void tearDown() throws IOException {
        client.indices().delete(new DeleteIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);
    }

    @Test
    public void testLargeUpdate() {
        final UpdateableDataContext dataContext = new ElasticSearchRestDataContext(client, INDEX_NAME);
        final Schema schema = dataContext.getDefaultSchema();
        final CreateTable createTable = new CreateTable(schema, DEFAULT_TABLE_NAME);
        createTable.withColumn("id").ofType(ColumnType.NUMBER);
        createTable.withColumn("firstname").ofType(ColumnType.STRING);
        createTable.withColumn("lastname").ofType(ColumnType.STRING);
        dataContext.executeUpdate(createTable);

        final Table table = schema.getTableByName(DEFAULT_TABLE_NAME);
        assertNotNull(table);

        dataContext.executeUpdate(new UpdateScript() {
            @Override
            public void run(final UpdateCallback callback) {
                IntStream
                        .range(1, 1000)
                        .forEach(id -> callback
                                .insertInto(table)
                                .value("id", id)
                                .value("firstname", "first" + id)
                                .execute());
            }
        });

        try (DataSet dataSet = dataContext.query().from(table).select("id", "firstname", "lastname").execute()) {
            dataSet.forEach(row -> {
                assertNotNull(row.getValue(0));
                assertNotNull(row.getValue(1));
                assertNull(row.getValue(2));
            });
        }

        dataContext.executeUpdate(new UpdateScript() {
            @Override
            public void run(final UpdateCallback callback) {
                IntStream
                        .range(1, 1000)
                        .forEach(id -> callback
                                .update(table)
                                .where("id")
                                .eq(id)
                                .value("lastname", "last" + id)
                                .execute());
            }
        });

        try (DataSet dataSet = dataContext.query().from(table).select("id", "firstname", "lastname").execute()) {
            dataSet.forEach(row -> {
                assertNotNull(row.getValue(0));
                assertNotNull(row.getValue(1));
                assertNotNull(row.getValue(2));
            });
        }
    }
}
