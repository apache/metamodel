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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.swing.table.TableModel;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetTableModel;
import org.apache.metamodel.data.FilteredDataSet;
import org.apache.metamodel.data.InMemoryDataSet;
import org.apache.metamodel.elasticsearch.utils.EmbeddedElasticsearchServer;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class ElasticSearchDataContextTest {

    private static final String indexName = "twitter";
    private static final String indexType1 = "tweet1";
    private static final String indexType2 = "tweet2";
    private static final String bulkIndexType = "bulktype";
    private static final String peopleIndexType = "peopletype";
    private static EmbeddedElasticsearchServer embeddedElasticsearchServer;
    private static Client client;
    private static DataContext dataContext;

    @BeforeClass
    public static void beforeTests() throws Exception {
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
        indexBulkDocuments(indexName, bulkIndexType, 10);

        // TODO: Find a better way than sleep to ensure data is in sync.

        // The refresh API allows to explicitly refresh one or more index,
        // making all operations performed since the last refresh available for search
        client.admin().indices().prepareRefresh().execute().actionGet();
        dataContext = new ElasticSearchDataContext(client, indexName);
        System.out.println("Embedded ElasticSearch server created!");
    }

    @AfterClass
    public static void afterTests() {
        embeddedElasticsearchServer.shutdown();
        System.out.println("Embedded ElasticSearch server shut down!");
    }

    @Test
    public void testSimpleQuery() throws Exception {
        assertEquals("[bulktype, peopletype, tweet1, tweet2]",
                Arrays.toString(dataContext.getDefaultSchema().getTableNames()));

        Table table = dataContext.getDefaultSchema().getTableByName("tweet1");

        assertEquals("[_id, message, postDate, user]", Arrays.toString(table.getColumnNames()));

        assertEquals(ColumnType.STRING, table.getColumnByName("user").getType());
        assertEquals(ColumnType.DATE, table.getColumnByName("postDate").getType());
        assertEquals(ColumnType.BIGINT, table.getColumnByName("message").getType());

        DataSet ds = dataContext.query().from(indexType1).select("user").and("message").execute();
        assertEquals(ElasticSearchDataSet.class, ds.getClass());

        try {
            assertTrue(ds.next());
            assertEquals("Row[values=[user1, 1]]", ds.getRow().toString());
        } finally {
            ds.close();
        }
    }

    @Test
    public void testDocumentIdAsPrimaryKey() throws Exception {
        Table table = dataContext.getDefaultSchema().getTableByName("tweet2");
        Column[] pks = table.getPrimaryKeys();
        assertEquals(1, pks.length);
        assertEquals("_id", pks[0].getName());

        DataSet ds = dataContext.query().from(table).select("user", "_id").orderBy("_id").asc().execute();
        try {
            assertTrue(ds.next());
            assertEquals("Row[values=[user1, tweet_tweet2_1]]", ds.getRow().toString());
        } finally {
            ds.close();
        }
    }

    @Test
    public void testExecutePrimaryKeyLookupQuery() throws Exception {
        Table table = dataContext.getDefaultSchema().getTableByName("tweet2");
        Column[] pks = table.getPrimaryKeys();

        DataSet ds = dataContext.query().from(table).selectAll().where(pks[0]).eq("tweet_tweet2_1").execute();
        try {
            assertTrue(ds.next());
            Object dateValue = ds.getRow().getValue(2);
            assertEquals("Row[values=[tweet_tweet2_1, 1, " + dateValue + ", user1]]", ds.getRow().toString());

            assertFalse(ds.next());

            assertEquals(InMemoryDataSet.class, ds.getClass());
        } finally {
            ds.close();
        }
    }

    // TODO: Un-ignore this test, and it wil fail - needs fixin'
    @Test
    @Ignore
    public void testDateIsHandledAsDate() throws Exception {
        Table table = dataContext.getDefaultSchema().getTableByName("tweet1");
        Column column = table.getColumnByName("postDate");
        ColumnType type = column.getType();
        assertEquals(ColumnType.DATE, type);

        DataSet dataSet = dataContext.query().from(table).select(column).execute();
        while (dataSet.next()) {
            Object value = dataSet.getRow().getValue(column);
            assertTrue("Got class: " + value.getClass() + ", expected Date (or subclass)", value instanceof Date);
        }
    }

    @Test
    public void testNumberIsHandledAsNumber() throws Exception {
        Table table = dataContext.getDefaultSchema().getTableByName("peopletype");
        Column column = table.getColumnByName("age");
        ColumnType type = column.getType();
        assertEquals(ColumnType.BIGINT, type);

        DataSet dataSet = dataContext.query().from(table).select(column).execute();
        while (dataSet.next()) {
            Object value = dataSet.getRow().getValue(column);
            assertTrue("Got class: " + value.getClass() + ", expected Number (or subclass)", value instanceof Number);
        }
    }

    @Test
    public void testWhereColumnEqualsValues() throws Exception {
        DataSet ds = dataContext.query().from(bulkIndexType).select("user").and("message").where("user")
                .isEquals("user4").execute();
        assertEquals(FilteredDataSet.class, ds.getClass());

        try {
            assertTrue(ds.next());
            assertEquals("Row[values=[user4, 4]]", ds.getRow().toString());
            assertFalse(ds.next());
        } finally {
            ds.close();
        }
    }

    @Test
    public void testWhereColumnInValues() throws Exception {
        DataSet ds = dataContext.query().from(bulkIndexType).select("user").and("message").where("user")
                .in("user4", "user5").orderBy("message").execute();

        try {
            assertTrue(ds.next());

            String row1 = ds.getRow().toString();
            assertEquals("Row[values=[user4, 4]]", row1);
            assertTrue(ds.next());

            String row2 = ds.getRow().toString();
            assertEquals("Row[values=[user5, 5]]", row2);

            assertFalse(ds.next());
        } finally {
            ds.close();
        }
    }

    @Test
    public void testGroupByQuery() throws Exception {
        Table table = dataContext.getDefaultSchema().getTableByName(peopleIndexType);

        Query q = new Query();
        q.from(table);
        q.groupBy(table.getColumnByName("gender"));
        q.select(new SelectItem(table.getColumnByName("gender")),
                new SelectItem(FunctionType.MAX, table.getColumnByName("age")),
                new SelectItem(FunctionType.MIN, table.getColumnByName("age")), new SelectItem(FunctionType.COUNT, "*",
                        "total"), new SelectItem(FunctionType.MIN, table.getColumnByName("id")).setAlias("firstId"));
        q.orderBy("gender");
        DataSet data = dataContext.executeQuery(q);
        assertEquals(
                "[peopletype.gender, MAX(peopletype.age), MIN(peopletype.age), COUNT(*) AS total, MIN(peopletype.id) AS firstId]",
                Arrays.toString(data.getSelectItems()));

        assertTrue(data.next());
        assertEquals("Row[values=[female, 20, 17, 5, 5]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[male, 19, 17, 4, 1]]", data.getRow().toString());
        assertFalse(data.next());
    }

    @Test
    public void testFilterOnNumberColumn() {
        Table table = dataContext.getDefaultSchema().getTableByName(bulkIndexType);
        Query q = dataContext.query().from(table).select("user").where("message").greaterThan(7).toQuery();
        DataSet data = dataContext.executeQuery(q);
        String[] expectations = new String[] { "Row[values=[user8]]", "Row[values=[user9]]" };

        assertTrue(data.next());
        assertTrue(Arrays.asList(expectations).contains(data.getRow().toString()));
        assertTrue(data.next());
        assertTrue(Arrays.asList(expectations).contains(data.getRow().toString()));
        assertFalse(data.next());
    }

    @Test
    public void testMaxRows() throws Exception {
        Table table = dataContext.getDefaultSchema().getTableByName(peopleIndexType);
        Query query = new Query().from(table).select(table.getColumns()).setMaxRows(5);
        DataSet dataSet = dataContext.executeQuery(query);

        TableModel tableModel = new DataSetTableModel(dataSet);
        assertEquals(5, tableModel.getRowCount());
    }

    @Test
    public void testCountQuery() throws Exception {
        Table table = dataContext.getDefaultSchema().getTableByName(bulkIndexType);
        Query q = new Query().selectCount().from(table);

        List<Object[]> data = dataContext.executeQuery(q).toObjectArrays();
        assertEquals(1, data.size());
        Object[] row = data.get(0);
        assertEquals(1, row.length);
        assertEquals("[10]", Arrays.toString(row));
    }

    @Test
    public void testQueryForANonExistingTable() throws Exception {
        boolean thrown = false;
        try {
            dataContext.query().from("nonExistingTable").select("user").and("message").execute();
        } catch (IllegalArgumentException IAex) {
            thrown = true;
        } finally {
            // ds.close();
        }
        assertTrue(thrown);
    }

    @Test
    public void testQueryForAnExistingTableAndNonExistingField() throws Exception {
        indexOneTweeterDocumentPerIndex(indexType1, 1);
        boolean thrown = false;
        try {
            dataContext.query().from(indexType1).select("nonExistingField").execute();
        } catch (IllegalArgumentException IAex) {
            thrown = true;
        } finally {
            // ds.close();
        }
        assertTrue(thrown);
    }

    private static void indexBulkDocuments(String indexName, String indexType, int numberOfDocuments) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        try {
            for (int i = 0; i < numberOfDocuments; i++) {
                bulkRequest.add(client.prepareIndex(indexName, indexType, new Integer(i).toString()).setSource(
                        buildTweeterJson(i)));
            }
            bulkRequest.execute().actionGet();
        } catch (Exception ex) {
            System.out.println("Exception indexing documents!!!!!");
        }

    }

    private static void indexOneTweeterDocumentPerIndex(String indexType, int id) {
        try {
            client.prepareIndex(indexName, indexType).setSource(buildTweeterJson(id))
                    .setId("tweet_" + indexType + "_" + id).execute().actionGet();
        } catch (Exception ex) {
            System.out.println("Exception indexing documents!!!!!");
        }
    }

    private static void indexOnePeopleDocument(String gender, int age, int id) {
        try {
            client.prepareIndex(indexName, peopleIndexType).setSource(buildPeopleJson(gender, age, id)).execute()
                    .actionGet();
        } catch (Exception ex) {
            System.out.println("Exception indexing documents!!!!!");
        }
    }

    private static Map<String, Object> buildTweeterJson(int elementId) throws Exception {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put("user", "user" + elementId);
        map.put("postDate", new Date());
        map.put("message", elementId);
        return map;
    }

    private static XContentBuilder buildPeopleJson(String gender, int age, int elementId) throws Exception {
        return jsonBuilder().startObject().field("gender", gender).field("age", age).field("id", elementId).endObject();
    }

}