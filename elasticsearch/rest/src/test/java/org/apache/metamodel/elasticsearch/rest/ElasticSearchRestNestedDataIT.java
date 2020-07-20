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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ElasticSearchRestNestedDataIT {
    private static final String INDEX_NAME = "nesteddata";

    private static RestHighLevelClient client;
    private static UpdateableDataContext dataContext;

    @Before
    public void setUp() throws Exception {
        final String dockerHostAddress = ElasticSearchRestDataContextIT.determineHostName();

        client = ElasticSearchRestUtil
                .createClient(new HttpHost(dockerHostAddress, ElasticSearchRestDataContextIT.DEFAULT_REST_CLIENT_PORT),
                        null, null);
        client.indices().create(new CreateIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);

        dataContext = new ElasticSearchRestDataContext(client, INDEX_NAME);
    }

    @After
    public void tearDown() throws IOException {
        client.indices().delete(new DeleteIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);
    }

    @Test
    public void testNestedData() throws Exception {
        final Map<String, Object> address = new HashMap<>();
        address.put("street", "Main street 1");
        address.put("city", "Newville");

        final Map<String, Object> user = new HashMap<>();
        user.put("fullname", "John Doe");
        user.put("address", address);

        final Map<String, Object> userMessage = new LinkedHashMap<>();
        userMessage.put("user", user);
        userMessage.put("message", "This is what I have to say.");

        final IndexRequest indexRequest = new IndexRequest(INDEX_NAME).id("1");
        indexRequest.source(userMessage);

        client.index(indexRequest, RequestOptions.DEFAULT);

        validateSchemaAndResults();
    }

    @Test
    public void testIndexOfDocumentWithDots() throws Exception {
        final String document =
                "{ \"user.fullname\": \"John Doe\", "
                + "\"user.address.street\": \"Main street 1\", "
                + "\"user.address.city\": \"Newville\", "
                + "\"message\": \"This is what I have to say.\" }";

        final IndexRequest indexRequest = new IndexRequest(INDEX_NAME).id("1");
        indexRequest.source(document, XContentType.JSON);

        client.index(indexRequest, RequestOptions.DEFAULT);

        validateSchemaAndResults();
    }

    private void validateSchemaAndResults() {
        final Table table = dataContext.getDefaultSchema().getTableByName(DEFAULT_TABLE_NAME);

        assertThat(table.getColumnNames(), containsInAnyOrder("_id", "message", "user"));

        assertEquals(ColumnType.STRING, table.getColumnByName("message").getType());
        assertEquals(ColumnType.MAP, table.getColumnByName("user").getType());

        dataContext.refreshSchemas();
        
        try (final DataSet dataSet = dataContext
                .query()
                .from(DEFAULT_TABLE_NAME)
                .select("user")
                .and("message")
                .execute()) {
            assertEquals(ElasticSearchRestDataSet.class, dataSet.getClass());

            assertTrue(dataSet.next());
            final Row row = dataSet.getRow();
            assertEquals("This is what I have to say.", row.getValue(table.getColumnByName("message")));
            
            final Object userValue = row.getValue(table.getColumnByName("user"));
            assertTrue(userValue instanceof Map);

            @SuppressWarnings("rawtypes")
            final Map userValueMap = (Map) userValue;
            assertEquals("John Doe", userValueMap.get("fullname"));

            final Object addressValue = userValueMap.get("address");
            assertTrue(addressValue instanceof Map);

            @SuppressWarnings("rawtypes")
            final Map addressValueMap = (Map) addressValue;
            assertEquals("Main street 1", addressValueMap.get("street"));
            assertEquals("Newville", addressValueMap.get("city"));
        }
    }
}
