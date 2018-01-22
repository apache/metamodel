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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.metamodel.BatchUpdateScript;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.factory.DataContextFactory;
import org.apache.metamodel.factory.DataContextPropertiesImpl;
import org.apache.metamodel.schema.ColumnType;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ElasticSearchRestDataContexFactoryIT {
    private static final String INDEX_NAME = "myindex";

    private static ElasticSearchRestClient externalClient;

    @Before
    public void setUp() throws Exception {
        final String dockerHostAddress = ElasticSearchRestDataContextIT.determineHostName();
        
        externalClient = new ElasticSearchRestClient(RestClient.builder(new HttpHost(dockerHostAddress, 9200)).build()); 

        final Map<String, Object> source = new LinkedHashMap<>();
        source.put("mytext", "dummy");
        
        final IndexRequest indexRequest = new IndexRequest(INDEX_NAME, "text");
        indexRequest.source(source);
        
        externalClient.index(indexRequest);
    }

    @After
    public void tearDown() throws IOException {
        externalClient.delete(INDEX_NAME);
    }

    @Test
    public void testAccepts() throws Exception {
        final String dockerHostAddress = ElasticSearchRestDataContextIT.determineHostName();

        DataContextFactory factory = new ElasticSearchRestDataContextFactory();

        final DataContextPropertiesImpl properties = new DataContextPropertiesImpl();
        properties.setDataContextType("elasticsearch");
        properties.put(DataContextPropertiesImpl.PROPERTY_URL, "http://" + dockerHostAddress + ":9200");
        properties.put(DataContextPropertiesImpl.PROPERTY_DATABASE, INDEX_NAME);
        
        assertTrue(factory.accepts(properties, null));
    }
    
    @Test
    public void testCreateContextAndBulkScript() throws Exception {
        final String dockerHostAddress = ElasticSearchRestDataContextIT.determineHostName();

        DataContextFactory factory = new ElasticSearchRestDataContextFactory();

        final DataContextPropertiesImpl properties = new DataContextPropertiesImpl();
        properties.setDataContextType("es-rest");
        properties.put(DataContextPropertiesImpl.PROPERTY_URL, "http://" + dockerHostAddress + ":9200");
        properties.put(DataContextPropertiesImpl.PROPERTY_DATABASE, INDEX_NAME);
        
        assertTrue(factory.accepts(properties, null));

        final ElasticSearchRestDataContext dataContext = (ElasticSearchRestDataContext) factory.create(properties, null);

        dataContext.executeUpdate(new BatchUpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.createTable(INDEX_NAME, "persons")
                        .withColumn("name").ofType(ColumnType.STRING)
                        .withColumn("age").ofType(ColumnType.INTEGER)
                        .execute();
            }
        });

        dataContext.executeUpdate(new BatchUpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto("persons").value("name", "John Doe").value("age", "42").execute();
                callback.insertInto("persons").value("name", "Jane Doe").value("age", "41").execute();
            }});

        dataContext.refreshSchemas();

        final DataSet persons = dataContext.executeQuery("SELECT name, age FROM persons");
        final List<Row> personData = persons.toRows();
        
        assertEquals(2, personData.size());
        assertThat(Arrays.asList(personData.get(0).getValues()), containsInAnyOrder("John Doe", "42"));
        assertThat(Arrays.asList(personData.get(1).getValues()), containsInAnyOrder("Jane Doe", "41"));
    }
}
