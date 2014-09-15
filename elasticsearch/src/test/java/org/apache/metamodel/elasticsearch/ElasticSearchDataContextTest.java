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
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.FilteredDataSet;
import org.apache.metamodel.elasticsearch.utils.EmbeddedElasticsearchServer;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.Arrays;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticSearchDataContextTest extends TestCase {

    private EmbeddedElasticsearchServer embeddedElasticsearchServer;
    private Client client;
    DataContext dataContext;
    String indexName = "twitter";
    String indexType1 = "tweet1";
    String indexType2 = "tweet2";
    String bulkIndexName = "bulktwitter";
    String bulkIndexType = "bulktype";
    String peopleIndexName = "peopleindex";
    String peopleIndexType = "peopletype";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        embeddedElasticsearchServer = new EmbeddedElasticsearchServer();
        client = embeddedElasticsearchServer.getClient();
        indexOneTweeterDocumentPerIndex(indexType1, 1);
        indexOneTweeterDocumentPerIndex(indexType2, 1);
        indexOnePeopleDocument("female", 20, 5);
        indexOnePeopleDocument("female", 17, 8);
        indexOnePeopleDocument("female", 18, 9);
        indexOnePeopleDocument("female", 19, 10);
        indexOnePeopleDocument("female", 20, 11);
        indexOnePeopleDocument("male", 19, 1);
        indexOnePeopleDocument("male", 17, 2);
        indexOnePeopleDocument("male", 18, 3);
        indexOnePeopleDocument("male", 18, 4);
        indexOneTweeterDocumentPerIndex(indexType2, 1);
        indexBulkDocuments(bulkIndexName, bulkIndexType, 10);
        // Waiting for indexing the data....
        Thread.sleep(2000);
        dataContext = new ElasticSearchDataContext(client);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        embeddedElasticsearchServer.shutdown();
    }

    public void testSimpleQuery() throws Exception {
        assertEquals("[peopletype, bulktype, tweet1, tweet2]", Arrays.toString(dataContext.getDefaultSchema().getTableNames()));

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
            //ds.close();
        }
    }

    public void testWhereColumnEqualsValues() throws Exception {
        DataSet ds = dataContext.query().from(bulkIndexType).select("user").and("message").where("user").isEquals("user4").execute();
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

    public void testWhereColumnInValues() throws Exception {
        DataSet ds = dataContext.query().from(bulkIndexType).select("user").and("message").where("user").in("user4", "user5").execute();
        assertEquals(FilteredDataSet.class, ds.getClass());

        try {
            assertTrue(ds.next());
            assertEquals("Row[values=[user4, 4]]",
                    ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[user5, 5]]",
                    ds.getRow().toString());
            assertFalse(ds.next());
        } finally {
            ds.close();
        }
    }

    public void testGroupByQuery() throws Exception {
        Table table = dataContext.getDefaultSchema().getTableByName(peopleIndexType);

        Query q = new Query();
        q.from(table);
        q.groupBy(table.getColumnByName("gender"));
        q.select(new SelectItem(table.getColumnByName("gender")),
                new SelectItem(FunctionType.MAX, table.getColumnByName("age")),
                new SelectItem(FunctionType.MIN, table.getColumnByName("age")), new SelectItem(FunctionType.COUNT, "*",
                "total"), new SelectItem(FunctionType.MIN, table.getColumnByName("id")).setAlias("firstId"));
        DataSet data = dataContext.executeQuery(q);
        assertEquals(
                "[peopletype.gender, MAX(peopletype.age), MIN(peopletype.age), COUNT(*) AS total, MIN(peopletype.id) AS firstId]",
                Arrays.toString(data.getSelectItems()));

        String[] expectations = new String[] { "Row[values=[female, 20, 17, 5, 5]]", "Row[values=[male, 19, 17, 4, 1]]" };

        assertTrue(data.next());
        assertTrue(Arrays.asList(expectations).contains(data.getRow().toString()));
        assertTrue(data.next());
        assertTrue(Arrays.asList(expectations).contains(data.getRow().toString()));
        assertFalse(data.next());
    }

    public void testQueryForANonExistingTable() throws Exception {
        boolean thrown = false;
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
        indexOneTweeterDocumentPerIndex(indexType1, 1);
        boolean thrown = false;
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
                    .setSource(buildTweeterJson(i)));
        }
        bulkRequest.execute().actionGet();
        } catch (Exception ex) {
           System.out.println("Exception indexing documents!!!!!");
        }

    }

    private void indexOneTweeterDocumentPerIndex(String indexType, int id) {
        try {
        client.prepareIndex(indexName, indexType)
                .setSource(buildTweeterJson(id))
                .execute()
                .actionGet();
        } catch (Exception ex) {
            System.out.println("Exception indexing documents!!!!!");
        }
    }

    private void indexOnePeopleDocument(String gender, int age, int id) {
        try {
            client.prepareIndex(peopleIndexName, peopleIndexType)
                    .setSource(buildPeopleJson(gender, age, id))
                    .execute()
                    .actionGet();
        } catch (Exception ex) {
            System.out.println("Exception indexing documents!!!!!");
        }
    }

    private XContentBuilder buildTweeterJson(int elementId) throws Exception {
        return jsonBuilder().startObject().field("user", "user" + elementId)
                .field("postDate", new Date())
                .field("message", elementId)
                .endObject();
    }

    private XContentBuilder buildPeopleJson(String gender, int age, int elementId) throws Exception {
        return jsonBuilder().startObject().field("gender", gender)
                .field("age", age)
                .field("id", elementId)
                .endObject();
    }


}