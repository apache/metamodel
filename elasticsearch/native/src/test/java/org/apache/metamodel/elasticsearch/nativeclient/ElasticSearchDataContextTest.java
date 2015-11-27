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
package org.apache.metamodel.elasticsearch.nativeclient;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.swing.table.TableModel;

import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.CreateTable;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetTableModel;
import org.apache.metamodel.data.InMemoryDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.delete.DeleteFrom;
import org.apache.metamodel.drop.DropTable;
import org.apache.metamodel.elasticsearch.common.ElasticSearchUtils;
import org.apache.metamodel.elasticsearch.nativeclient.utils.EmbeddedElasticsearchServer;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.query.parser.QueryParserException;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.Update;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ElasticSearchDataContextTest {

    private static final String indexName = "twitter";
    private static final String indexType1 = "tweet1";
    private static final String indexType2 = "tweet2";
    private static final String indexName2 = "twitter2";
    private static final String indexType3 = "tweet3";
    private static final String bulkIndexType = "bulktype";
    private static final String peopleIndexType = "peopletype";
    private static final String mapping = "{\"date_detection\":\"false\",\"properties\":{\"message\":{\"type\":\"string\",\"index\":\"not_analyzed\",\"doc_values\":\"true\"}}}";
    private static EmbeddedElasticsearchServer embeddedElasticsearchServer;
    private static Client client;
    private static UpdateableDataContext dataContext;

    @BeforeClass
    public static void beforeTests() throws Exception {
        embeddedElasticsearchServer = new EmbeddedElasticsearchServer();
        client = embeddedElasticsearchServer.getClient();
        indexTweeterDocument(indexType1, 1);
        indexTweeterDocument(indexType2, 1);
        indexTweeterDocument(indexType2, 2, null);
        insertPeopleDocuments();
        indexTweeterDocument(indexType2, 1);
        indexBulkDocuments(indexName, bulkIndexType, 10);

        // The refresh API allows to explicitly refresh one or more index,
        // making all operations performed since the last refresh available for
        // search
        embeddedElasticsearchServer.getClient().admin().indices().prepareRefresh().execute().actionGet();
        dataContext = new ElasticSearchDataContext(client, indexName);
        System.out.println("Embedded ElasticSearch server created!");
    }

    private static void insertPeopleDocuments() throws IOException {
        indexOnePeopleDocument("female", 20, 5);
        indexOnePeopleDocument("female", 17, 8);
        indexOnePeopleDocument("female", 18, 9);
        indexOnePeopleDocument("female", 19, 10);
        indexOnePeopleDocument("female", 20, 11);
        indexOnePeopleDocument("male", 19, 1);
        indexOnePeopleDocument("male", 17, 2);
        indexOnePeopleDocument("male", 18, 3);
        indexOnePeopleDocument("male", 18, 4);
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

        try(DataSet ds = dataContext.query().from(indexType1).select("user").and("message").execute()) {
            assertEquals(ElasticSearchDataSet.class, ds.getClass());

            assertTrue(ds.next());
            assertEquals("Row[values=[user1, 1]]", ds.getRow().toString());
        }
    }

    @Test
    public void testDocumentIdAsPrimaryKey() throws Exception {
        Table table = dataContext.getDefaultSchema().getTableByName("tweet2");
        Column[] pks = table.getPrimaryKeys();
        assertEquals(1, pks.length);
        assertEquals("_id", pks[0].getName());

        try (DataSet ds = dataContext.query().from(table).select("user", "_id").orderBy("_id").asc().execute()) {
            assertTrue(ds.next());
            assertEquals("Row[values=[user1, tweet_tweet2_1]]", ds.getRow().toString());
        }
    }

    @Test
    public void testExecutePrimaryKeyLookupQuery() throws Exception {
        Table table = dataContext.getDefaultSchema().getTableByName("tweet2");
        Column[] pks = table.getPrimaryKeys();

        try (DataSet ds = dataContext.query().from(table).selectAll().where(pks[0]).eq("tweet_tweet2_1").execute()) {
            assertTrue(ds.next());
            Object dateValue = ds.getRow().getValue(2);
            assertEquals("Row[values=[tweet_tweet2_1, 1, " + dateValue + ", user1]]", ds.getRow().toString());

            assertFalse(ds.next());

            assertEquals(InMemoryDataSet.class, ds.getClass());
        }
    }

    @Test
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
        Table table = dataContext.getDefaultSchema().getTableByName(peopleIndexType);
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
    public void testCreateTableInsertQueryAndDrop() throws Exception {
        final Schema schema = dataContext.getDefaultSchema();
        final CreateTable createTable = new CreateTable(schema, "testCreateTable");
        createTable.withColumn("foo").ofType(ColumnType.STRING);
        createTable.withColumn("bar").ofType(ColumnType.NUMBER);
        dataContext.executeUpdate(createTable);

        final Table table = schema.getTableByName("testCreateTable");
        assertEquals("[" + ElasticSearchUtils.FIELD_ID + ", foo, bar]", Arrays.toString(table.getColumnNames()));

        final Column fooColumn = table.getColumnByName("foo");
        final Column idColumn = table.getPrimaryKeys()[0];
        assertEquals("Column[name=_id,columnNumber=0,type=STRING,nullable=null,nativeType=null,columnSize=null]",
                idColumn.toString());

        dataContext.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value("foo", "hello").value("bar", 42).execute();
                callback.insertInto(table).value("foo", "world").value("bar", 43).execute();
            }
        });

        dataContext.refreshSchemas();

        try (DataSet ds = dataContext.query().from(table).selectAll().orderBy("bar").execute()) {
            assertTrue(ds.next());
            assertEquals("hello", ds.getRow().getValue(fooColumn).toString());
            assertNotNull(ds.getRow().getValue(idColumn));
            assertTrue(ds.next());
            assertEquals("world", ds.getRow().getValue(fooColumn).toString());
            assertNotNull(ds.getRow().getValue(idColumn));
            assertFalse(ds.next());
        }

        dataContext.executeUpdate(new DropTable(table));

        dataContext.refreshSchemas();

        assertNull(dataContext.getTableByQualifiedLabel(table.getName()));
    }

    @Test
    public void testDetectOutsideChanges() throws Exception {
        ElasticSearchDataContext elasticSearchDataContext = (ElasticSearchDataContext) dataContext;

        // Create the type in ES
        final IndicesAdminClient indicesAdmin = elasticSearchDataContext.getElasticSearchClient().admin().indices();
        final String tableType = "outsideTable";

        Object[] sourceProperties = { "testA", "type=string, store=true", "testB", "type=string, store=true" };

        new PutMappingRequestBuilder(indicesAdmin).setIndices(indexName).setType(tableType).setSource(sourceProperties)
                .execute().actionGet();

        dataContext.refreshSchemas();

        assertNotNull(dataContext.getDefaultSchema().getTableByName(tableType));

        new DeleteMappingRequestBuilder(indicesAdmin).setIndices(indexName).setType(tableType).execute().actionGet();
        dataContext.refreshSchemas();
        assertNull(dataContext.getTableByQualifiedLabel(tableType));
    }

    @Test
    public void testDeleteAll() throws Exception {
        final Schema schema = dataContext.getDefaultSchema();
        final CreateTable createTable = new CreateTable(schema, "testCreateTable");
        createTable.withColumn("foo").ofType(ColumnType.STRING);
        createTable.withColumn("bar").ofType(ColumnType.NUMBER);
        dataContext.executeUpdate(createTable);

        final Table table = schema.getTableByName("testCreateTable");

        dataContext.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value("foo", "hello").value("bar", 42).execute();
                callback.insertInto(table).value("foo", "world").value("bar", 43).execute();
            }
        });

        dataContext.executeUpdate(new DeleteFrom(table));

        Row row = MetaModelHelper.executeSingleRowQuery(dataContext, dataContext.query().from(table).selectCount()
                .toQuery());
        assertEquals("Row[values=[0]]", row.toString());

        dataContext.executeUpdate(new DropTable(table));
    }

    @Test
    public void testDeleteByQuery() throws Exception {
        final Schema schema = dataContext.getDefaultSchema();
        final CreateTable createTable = new CreateTable(schema, "testCreateTable");
        createTable.withColumn("foo").ofType(ColumnType.STRING);
        createTable.withColumn("bar").ofType(ColumnType.NUMBER);
        dataContext.executeUpdate(createTable);

        final Table table = schema.getTableByName("testCreateTable");

        dataContext.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value("foo", "hello").value("bar", 42).execute();
                callback.insertInto(table).value("foo", "world").value("bar", 43).execute();
            }
        });

        dataContext.executeUpdate(new DeleteFrom(table).where("foo").eq("hello").where("bar").eq(42));

        Row row = MetaModelHelper.executeSingleRowQuery(dataContext,
                dataContext.query().from(table).select("foo", "bar").toQuery());
        assertEquals("Row[values=[world, 43]]", row.toString());

        dataContext.executeUpdate(new DropTable(table));
    }

    @Test
    public void testDeleteUnsupportedQueryType() throws Exception {
        final Schema schema = dataContext.getDefaultSchema();
        final CreateTable createTable = new CreateTable(schema, "testCreateTable");
        createTable.withColumn("foo").ofType(ColumnType.STRING);
        createTable.withColumn("bar").ofType(ColumnType.NUMBER);
        dataContext.executeUpdate(createTable);

        final Table table = schema.getTableByName("testCreateTable");
        try {

            dataContext.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback callback) {
                    callback.insertInto(table).value("foo", "hello").value("bar", 42).execute();
                    callback.insertInto(table).value("foo", "world").value("bar", 43).execute();
                }
            });

            // greater than is not yet supported
            try {
                dataContext.executeUpdate(new DeleteFrom(table).where("bar").gt(40));
                fail("Exception expected");
            } catch (UnsupportedOperationException e) {
                assertEquals("Could not push down WHERE items to delete by query request: [testCreateTable.bar > 40]",
                        e.getMessage());
            }

        } finally {
            dataContext.executeUpdate(new DropTable(table));
        }
    }

    @Test
    public void testUpdateRow() throws Exception {
        final Schema schema = dataContext.getDefaultSchema();
        final CreateTable createTable = new CreateTable(schema, "testCreateTable");
        createTable.withColumn("foo").ofType(ColumnType.STRING);
        createTable.withColumn("bar").ofType(ColumnType.NUMBER);
        dataContext.executeUpdate(createTable);

        final Table table = schema.getTableByName("testCreateTable");
        try {

            dataContext.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback callback) {
                    callback.insertInto(table).value("foo", "hello").value("bar", 42).execute();
                    callback.insertInto(table).value("foo", "world").value("bar", 43).execute();
                }
            });

            dataContext.executeUpdate(new Update(table).value("foo", "howdy").where("bar").eq(42));

            DataSet dataSet = dataContext.query().from(table).select("foo", "bar").orderBy("bar").execute();
            assertTrue(dataSet.next());
            assertEquals("Row[values=[howdy, 42]]", dataSet.getRow().toString());
            assertTrue(dataSet.next());
            assertEquals("Row[values=[world, 43]]", dataSet.getRow().toString());
            assertFalse(dataSet.next());
            dataSet.close();
        } finally {
            dataContext.executeUpdate(new DropTable(table));
        }
    }

    @Test
    public void testDropTable() throws Exception {
        Table table = dataContext.getDefaultSchema().getTableByName(peopleIndexType);

        // assert that the table was there to begin with
        {
            DataSet ds = dataContext.query().from(table).selectCount().execute();
            ds.next();
            assertEquals("Row[values=[9]]", ds.getRow().toString());
            ds.close();
        }

        dataContext.executeUpdate(new DropTable(table));
        try {
            DataSet ds = dataContext.query().from(table).selectCount().execute();
            ds.next();
            assertEquals("Row[values=[0]]", ds.getRow().toString());
            ds.close();
        } finally {
            // restore the people documents for the next tests
            insertPeopleDocuments();
            client.admin().indices().prepareRefresh().execute().actionGet();
            dataContext = new ElasticSearchDataContext(client, indexName);
        }
    }

    @Test
    public void testWhereColumnEqualsValues() throws Exception {
        try (DataSet ds = dataContext.query().from(bulkIndexType).select("user").and("message").where("user")
                .isEquals("user4").execute()) {
            assertEquals(ElasticSearchDataSet.class, ds.getClass());

            assertTrue(ds.next());
            assertEquals("Row[values=[user4, 4]]", ds.getRow().toString());
            assertFalse(ds.next());
        }
    }

    @Test
    public void testWhereColumnIsNullValues() throws Exception {
        try(DataSet ds = dataContext.query().from(indexType2).select("message").where("postDate")
                .isNull().execute()){
            assertEquals(ElasticSearchDataSet.class, ds.getClass());

            assertTrue(ds.next());
            assertEquals("Row[values=[2]]", ds.getRow().toString());
            assertFalse(ds.next());
        }
    }

    @Test
    public void testWhereColumnIsNotNullValues() throws Exception {
        try(DataSet ds = dataContext.query().from(indexType2).select("message").where("postDate")
                .isNotNull().execute()){
            assertEquals(ElasticSearchDataSet.class, ds.getClass());

            assertTrue(ds.next());
            assertEquals("Row[values=[1]]", ds.getRow().toString());
            assertFalse(ds.next());
        }
    }

    @Test
    public void testWhereMultiColumnsEqualValues() throws Exception {
        try(DataSet ds = dataContext.query().from(bulkIndexType).select("user").and("message").where("user")
                .isEquals("user4").and("message").ne(5).execute()){
            assertEquals(ElasticSearchDataSet.class, ds.getClass());

            assertTrue(ds.next());
            assertEquals("Row[values=[user4, 4]]", ds.getRow().toString());
            assertFalse(ds.next());
        }
    }

    @Test
    public void testWhereColumnInValues() throws Exception {
        try (DataSet ds = dataContext.query().from(bulkIndexType).select("user").and("message").where("user")
                .in("user4", "user5").orderBy("message").execute()) {
            assertTrue(ds.next());

            String row1 = ds.getRow().toString();
            assertEquals("Row[values=[user4, 4]]", row1);
            assertTrue(ds.next());

            String row2 = ds.getRow().toString();
            assertEquals("Row[values=[user5, 5]]", row2);

            assertFalse(ds.next());
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

    @Test(expected = IllegalArgumentException.class)
    public void testQueryForANonExistingTable() throws Exception {
        dataContext.query().from("nonExistingTable").select("user").and("message").execute();
    }

    @Test(expected = QueryParserException.class)
    public void testQueryForAnExistingTableAndNonExistingField() throws Exception {
        indexTweeterDocument(indexType1, 1);
        dataContext.query().from(indexType1).select("nonExistingField").execute();
    }

    @Test
    public void testNonDynamicMapingTableNames() throws Exception {
        createIndex();

        ElasticSearchDataContext dataContext2 = new ElasticSearchDataContext(client, indexName2);

        assertEquals("[tweet3]", Arrays.toString(dataContext2.getDefaultSchema().getTableNames()));
    }

    private static void createIndex() {
        CreateIndexRequest cir = new CreateIndexRequest(indexName2);
        CreateIndexResponse response = client.admin().indices().create(cir).actionGet();

        System.out.println("create index: " + response.isAcknowledged());

        PutMappingRequest pmr = new PutMappingRequest(indexName2).type(indexType3).source(mapping);

        PutMappingResponse response2 = client.admin().indices().putMapping(pmr).actionGet();
        System.out.println("put mapping: " + response2.isAcknowledged());
    }

    private static void indexBulkDocuments(String indexName, String indexType, int numberOfDocuments) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        for (int i = 0; i < numberOfDocuments; i++) {
            bulkRequest.add(client.prepareIndex(indexName, indexType, Integer.toString(i)).setSource(
                    buildTweeterJson(i)));
        }
        bulkRequest.execute().actionGet();
    }

    private static void indexTweeterDocument(String indexType, int id, Date date) {
        client.prepareIndex(indexName, indexType).setSource(buildTweeterJson(id, date))
                .setId("tweet_" + indexType + "_" + id).execute().actionGet();
    }

    private static void indexTweeterDocument(String indexType, int id) {
        client.prepareIndex(indexName, indexType).setSource(buildTweeterJson(id))
                .setId("tweet_" + indexType + "_" + id).execute().actionGet();
    }

    private static void indexOnePeopleDocument(String gender, int age, int id) throws IOException {
        client.prepareIndex(indexName, peopleIndexType).setSource(buildPeopleJson(gender, age, id)).execute()
                .actionGet();
    }

    private static Map<String, Object> buildTweeterJson(int elementId) {
        return buildTweeterJson(elementId, new Date());
    }

    private static Map<String, Object> buildTweeterJson(int elementId, Date date) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("user", "user" + elementId);
        map.put("postDate", date);
        map.put("message", elementId);
        return map;
    }

    private static XContentBuilder buildPeopleJson(String gender, int age, int elementId) throws IOException {
        return jsonBuilder().startObject().field("gender", gender).field("age", age).field("id", elementId).endObject();
    }

}
