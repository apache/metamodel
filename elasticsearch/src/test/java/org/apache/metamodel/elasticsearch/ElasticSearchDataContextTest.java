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
package org.apache.metamodel.elasticsearch;

import junit.framework.TestCase;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.FilteredDataSet;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.apache.metamodel.elasticsearch.utils.EmbeddedElasticsearchServer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticSearchDataContextTest extends TestCase {

    private EmbeddedElasticsearchServer embeddedElasticsearchServer;
    private Client client;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        embeddedElasticsearchServer = new EmbeddedElasticsearchServer();
        client = embeddedElasticsearchServer.getClient();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        embeddedElasticsearchServer.shutdown();
    }

    protected Client getClient() {
        return embeddedElasticsearchServer.getClient();
    }

    public void testSimpleQuery() throws Exception {
        String indexName = "twitter";
        String indexType1 = "tweet1";
        String indexType2 = "tweet2";
        indexOneDocumentPerIndex(indexName, indexType1, 1);
        indexOneDocumentPerIndex(indexName, indexType2, 1);
        // Waiting for indexing the data....
        Thread.sleep(2000);

        final DataContext dataContext = new ElasticSearchDataContext(client);

        assertEquals("[tweet1, tweet2]", Arrays.toString(dataContext.getDefaultSchema().getTableNames()));

        Table table = dataContext.getDefaultSchema().getTableByName("tweet1");

        assertEquals(ColumnType.VARCHAR, table.getColumnByName("user").getType());
        assertEquals(ColumnType.DATE, table.getColumnByName("postDate").getType());
        assertEquals(ColumnType.BIGINT, table.getColumnByName("message").getType());

        DataSet ds = dataContext.query().from(indexType1).select("user").and("message").execute();
        assertEquals(ElasticSearchDataSet.class, ds.getClass());
        assertFalse(((ElasticSearchDataSet) ds).isQueryPostProcessed());

        try {
            assertTrue(ds.next());
            assertEquals("Row[values=[user1, 1]]",
                    ds.getRow().toString());
        } finally {
            ds.close();
        }
    }

    public void testQueryWithEqualsWhereClause() throws Exception {
        String indexName = "twitter";
        String indexType = "tweet1";
        indexBulkDocuments(indexName, indexType, 10);
        // Waiting for indexing the data....
        Thread.sleep(2000);

        final DataContext dataContext = new ElasticSearchDataContext(client);
        DataSet ds = dataContext.query().from(indexType).select("user").and("message").where("user").isEquals("user4").execute();
        assertEquals(FilteredDataSet.class, ds.getClass());

        try {
            assertTrue(ds.next());
            assertEquals("Row[values=[user4, 4]]",
                    ds.getRow().toString());
            assertFalse(ds.next());
        } finally {
            ds.close();
        }
    }

    public void testQueryForANonExistingTable() throws Exception {
        boolean thrown = false;
        final DataContext dataContext = new ElasticSearchDataContext(client);
        try {
            dataContext.query().from("nonExistingTable").select("user").and("message").execute();
        } catch(IllegalArgumentException IAex) {
            thrown = true;
        } finally {
            //ds.close();
        }
        assertTrue(thrown);
    }

    public void testQueryForAnExistingTableAndNonExistingField() throws Exception {
        String indexName = "twitter";
        String indexType1 = "tweet1";
        indexOneDocumentPerIndex(indexName, indexType1, 1);
        boolean thrown = false;
        final DataContext dataContext = new ElasticSearchDataContext(client);
        try {
            dataContext.query().from(indexType1).select("nonExistingField").execute();
        } catch(IllegalArgumentException IAex) {
            thrown = true;
        } finally {
            //ds.close();
        }
        assertTrue(thrown);
    }

    private void indexBulkDocuments(String indexName, String indexType, int numberOfDocuments) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        try {
        for (int i = 0; i < numberOfDocuments; i++) {
            bulkRequest.add(client.prepareIndex(indexName, indexType, new Integer(i).toString())
                    .setSource(buildJsonObject(i)));
        }
        bulkRequest.execute().actionGet();
        } catch (Exception ex) {
           System.out.println("Exception indexing documents!!!!!");
        }

    }

    private void indexOneDocumentPerIndex(String indexName, String indexType, int id) {
        try {
        client.prepareIndex(indexName, indexType)
                .setSource(buildJsonObject(id))
                .execute()
                .actionGet();
        } catch (Exception ex) {
            System.out.println("Exception indexing documents!!!!!");
        }
    }

    private XContentBuilder buildJsonObject(int elementId) throws Exception {
        return jsonBuilder().startObject().field("user", "user" + elementId)
                .field("postDate", new Date())
                .field("message", elementId)
                .endObject();
    }
}