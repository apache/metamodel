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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.http.HttpHost;
import org.apache.metamodel.BatchUpdateScript;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.elasticsearch.common.ElasticSearchUtils;
import org.apache.metamodel.factory.DataContextFactory;
import org.apache.metamodel.factory.DataContextPropertiesImpl;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.TableType;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ElasticSearchRestDataContexFactoryIT {
    private static final String INDEX_NAME = "myindex";

    private static RestHighLevelClient externalClient;

    private String dockerHostAddress;

    private DataContextFactory factory;
    
    private DataContextPropertiesImpl properties;

    @Before
    public void setUp() throws Exception {
        dockerHostAddress = ElasticSearchRestDataContextIT.determineHostName();

        externalClient = new RestHighLevelClient(RestClient
                .builder(new HttpHost(dockerHostAddress, ElasticSearchRestDataContextIT.DEFAULT_REST_CLIENT_PORT)));
        externalClient.indices().create(new CreateIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);

        final MutableTable table = new MutableTable(DEFAULT_TABLE_NAME, TableType.TABLE, new MutableSchema(INDEX_NAME),
                new MutableColumn("name", ColumnType.STRING), new MutableColumn("age", ColumnType.INTEGER));
        final PutMappingRequest putMappingRequest = new PutMappingRequest(INDEX_NAME)
                .source(ElasticSearchUtils.getMappingSource(table));

        externalClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);

        factory = new ElasticSearchRestDataContextFactory();

        properties = new DataContextPropertiesImpl();
        properties
                .put(DataContextPropertiesImpl.PROPERTY_URL, "http://" + dockerHostAddress + ":"
                        + ElasticSearchRestDataContextIT.DEFAULT_REST_CLIENT_PORT);
        properties.put(DataContextPropertiesImpl.PROPERTY_DATABASE, INDEX_NAME);
    }

    @After
    public void tearDown() throws IOException {
        externalClient.indices().delete(new DeleteIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);
    }

    @Test
    public void testAccepts() throws Exception {
        properties.setDataContextType("elasticsearch");

        assertTrue(factory.accepts(properties, null));
    }

    @Test
    public void testCreateContextAndBulkScript() throws Exception {
        properties.setDataContextType("es-rest");

        assertTrue(factory.accepts(properties, null));

        final ElasticSearchRestDataContext dataContext = (ElasticSearchRestDataContext) factory
                .create(properties, null);

        dataContext.executeUpdate(new BatchUpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(DEFAULT_TABLE_NAME).value("name", "John Doe").value("age", 42).execute();
                callback.insertInto(DEFAULT_TABLE_NAME).value("name", "Jane Doe").value("age", 41).execute();
            }
        });

        dataContext.refreshSchemas();

        final DataSet persons = dataContext.executeQuery("SELECT name, age FROM " + DEFAULT_TABLE_NAME);
        final List<Row> personData = persons.toRows();

        assertEquals(2, personData.size());

        // Sort person data, so we can validate each row's values.
        final Column ageColumn = dataContext
                .getSchemaByName(INDEX_NAME)
                .getTableByName(DEFAULT_TABLE_NAME)
                .getColumnByName("age");
        personData
                .sort((row1, row2) -> ((Integer) row1.getValue(ageColumn))
                        .compareTo(((Integer) row2.getValue(ageColumn))));

        assertThat(Arrays.asList(personData.get(0).getValues()), containsInAnyOrder("Jane Doe", 41));
        assertThat(Arrays.asList(personData.get(1).getValues()), containsInAnyOrder("John Doe", 42));
    }

}
