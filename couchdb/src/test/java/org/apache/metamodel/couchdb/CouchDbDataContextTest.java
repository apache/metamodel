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
package org.apache.metamodel.couchdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.drop.DropTable;
import org.apache.metamodel.insert.InsertInto;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;
import org.ektorp.CouchDbConnector;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

public class CouchDbDataContextTest extends CouchDbTestCase {

    private HttpClient httpClient;
    private StdCouchDbInstance couchDbInstance;
    private CouchDbConnector connector;
    private SimpleTableDef predefinedTableDef;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        if (isConfigured()) {
            final int timeout = 8 * 1000; // 8 seconds should be more than enough
            httpClient = new StdHttpClient.Builder().socketTimeout(timeout).host(getHostname()).build();

            // set up a simple database
            couchDbInstance = new StdCouchDbInstance(httpClient);

            final String databaseName = getDatabaseName();
            if (couchDbInstance.getAllDatabases().contains(databaseName)) {
                throw new IllegalStateException("Couch DB instance already has a database called " + databaseName);
            }
            connector = couchDbInstance.createConnector(databaseName, true);

            final String[] columnNames = new String[] { "name", "gender", "age" };
            final ColumnType[] columnTypes = new ColumnType[] { ColumnType.STRING, ColumnType.CHAR, ColumnType.INTEGER };
            predefinedTableDef = new SimpleTableDef(databaseName, columnNames, columnTypes);
        }

    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();

        connector = null;

        if (isConfigured()) {
            couchDbInstance.deleteDatabase(getDatabaseName());

            httpClient.shutdown();
        }
    }

    public void testWorkingWithMapsAndLists() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        connector = couchDbInstance.createConnector("test_table_map_and_list", true);

        final CouchDbDataContext dc = new CouchDbDataContext(couchDbInstance, new SimpleTableDef(
                "test_table_map_and_list", new String[] { "id", "foo", "bar" }, new ColumnType[] { ColumnType.INTEGER,
                        ColumnType.MAP, ColumnType.LIST }));
        Table table = null;
        try {
            table = dc.getTableByQualifiedLabel("test_table_map_and_list");
            Map<String, Object> exampleMap = new LinkedHashMap<String, Object>();
            exampleMap.put("hello", Arrays.asList("world", "welt", "verden"));
            exampleMap.put("foo", "bar");

            List<Map<String, Object>> exampleList = new ArrayList<Map<String, Object>>();
            exampleList.add(new LinkedHashMap<String, Object>());
            Map<String, Object> exampleMap2 = new LinkedHashMap<String, Object>();
            exampleMap2.put("meta", "model");
            exampleMap2.put("couch", "db");
            exampleList.add(exampleMap2);

            dc.executeUpdate(new InsertInto(table).value("id", 1).value("foo", exampleMap).value("bar", exampleList));

            DataSet ds = dc.query().from(table).select("id", "foo", "bar").execute();
            assertTrue(ds.next());
            Row row = ds.getRow();
            assertFalse(ds.next());
            ds.close();

            assertEquals("Row[values=[1, {hello=[world, welt, verden], foo=bar}, [{}, {meta=model, couch=db}]]]",
                    row.toString());
            assertTrue(row.getValue(0) instanceof Integer);
            assertTrue(row.getValue(1) instanceof Map);
            assertTrue(row.getValue(2) instanceof List);

            CouchDbDataContext dc2 = new CouchDbDataContext(couchDbInstance, new SimpleTableDef("test_table_map_and_list",
                    new String[] { "foo.hello[0]", "bar[1].couch" }));
            ds = dc2.query().from("test_table_map_and_list").select("foo.hello[0]", "bar[1].couch").execute();
            assertTrue(ds.next());
            assertEquals("Row[values=[world, db]]", ds.getRow().toString());
            assertFalse(ds.next());
            ds.close();

        } finally {
            dc.executeUpdate(new DropTable(table));
        }

    }

    public void testCreateUpdateDeleteScenario() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        final String databaseName = getDatabaseName();

        {
            // insert a document to provide data to do inferential schema
            // detection
            final CouchDbConnector connector = couchDbInstance.createConnector(databaseName, false);
            final Map<String, Object> map = new HashMap<String, Object>();
            map.put("foo", "bar");
            map.put("bar", "baz");
            map.put("baz", 1234);
            connector.addToBulkBuffer(map);
            connector.flushBulkBuffer();
        }

        final CouchDbDataContext dc = new CouchDbDataContext(couchDbInstance);
        Table table = dc.getDefaultSchema().getTableByName(databaseName);
        assertNotNull(table);
        assertEquals("[Column[name=_id,columnNumber=0,type=STRING,nullable=false,nativeType=null,columnSize=null], "
                + "Column[name=_rev,columnNumber=1,type=STRING,nullable=false,nativeType=null,columnSize=null], "
                + "Column[name=bar,columnNumber=2,type=STRING,nullable=null,nativeType=null,columnSize=null], "
                + "Column[name=baz,columnNumber=3,type=INTEGER,nullable=null,nativeType=null,columnSize=null], "
                + "Column[name=foo,columnNumber=4,type=STRING,nullable=null,nativeType=null,columnSize=null]]",
                Arrays.toString(table.getColumns().toArray()));

        // first delete the manually created database!
        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.dropTable(databaseName).execute();
            }
        });

        table = dc.getDefaultSchema().getTableByName(databaseName);
        assertNull(table);

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                Table table = callback.createTable(dc.getDefaultSchema(), databaseName).withColumn("foo")
                        .ofType(ColumnType.STRING).withColumn("greeting").ofType(ColumnType.STRING).execute();
                assertEquals("[_id, _rev, foo, greeting]", Arrays.toString(table.getColumnNames().toArray()));
            }
        });

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(databaseName).value("foo", "bar").value("greeting", "hello").execute();
                callback.insertInto(databaseName).value("foo", "baz").value("greeting", "hi").execute();
            }
        });

        DataSet ds = dc.query().from(databaseName).select("_id", "foo", "greeting").execute();
        assertTrue(ds.next());
        assertNotNull(ds.getRow().getValue(0));
        assertEquals("bar", ds.getRow().getValue(1));
        assertEquals("hello", ds.getRow().getValue(2));
        assertTrue(ds.next());
        assertNotNull(ds.getRow().getValue(0));
        assertEquals("baz", ds.getRow().getValue(1));
        assertEquals("hi", ds.getRow().getValue(2));
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.update(databaseName).value("greeting", "howdy").where("foo").isEquals("baz").execute();

                callback.update(databaseName).value("foo", "foo").where("foo").isEquals("bar").execute();
            }
        });

        ds = dc.query().from(databaseName).select("_id", "foo", "greeting").execute();
        assertTrue(ds.next());
        assertNotNull(ds.getRow().getValue(0));
        assertEquals("foo", ds.getRow().getValue(1));
        assertEquals("hello", ds.getRow().getValue(2));
        assertTrue(ds.next());
        assertNotNull(ds.getRow().getValue(0));
        assertEquals("baz", ds.getRow().getValue(1));
        assertEquals("howdy", ds.getRow().getValue(2));
        assertFalse(ds.next());
        ds.close();
    }

    public void testBasicQuery() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        // insert a few records
        {
            HashMap<String, Object> map;

            map = new HashMap<String, Object>();
            map.put("name", "John Doe");
            map.put("age", 30);
            connector.create(map);

            map = new HashMap<String, Object>();
            map.put("name", "Jane Doe");
            map.put("gender", 'F');
            connector.create(map);
        }

        // create datacontext using detected schema
        CouchDbDataContext dc = new CouchDbDataContext(couchDbInstance, getDatabaseName());

        // verify schema and execute query
        Schema schema = dc.getMainSchema();
        assertEquals("[" + getDatabaseName() + "]", Arrays.toString(schema.getTableNames().toArray()));

        assertEquals("[_id, _rev, age, gender, name]",
                Arrays.toString(schema.getTableByName(getDatabaseName()).getColumnNames().toArray()));
        Column idColumn = schema.getTableByName(getDatabaseName()).getColumnByName("_id");
        assertEquals("Column[name=_id,columnNumber=0,type=STRING,nullable=false,nativeType=null,columnSize=null]",
                idColumn.toString());
        assertTrue(idColumn.isPrimaryKey());

        assertEquals("Column[name=_rev,columnNumber=1,type=STRING,nullable=false,nativeType=null,columnSize=null]",
                schema.getTableByName(getDatabaseName()).getColumnByName("_rev").toString());

        DataSet ds;

        ds = dc.query().from(getDatabaseName()).select("name").and("age").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[John Doe, 30]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[Jane Doe, null]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        ds = dc.query().from(getDatabaseName()).select("name").and("gender").where("age").isNull().execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[Jane Doe, F]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        ds = dc.query().from(getDatabaseName()).select("_id").where("name").eq("Jane Doe").execute();
        assertTrue(ds.next());
        Object pkValue = ds.getRow().getValue(0);
        assertFalse(ds.next());
        ds.close();

        // Test greater than or equals
        ds = dc.query().from(getDatabaseName()).select("name").and("age").where("age").greaterThanOrEquals(29)
                .execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[John Doe, 30]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        ds = dc.query().from(getDatabaseName()).select("name").and("age").where("age").greaterThanOrEquals(30)
                .execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[John Doe, 30]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        ds = dc.query().from(getDatabaseName()).select("name").and("age").where("age").greaterThanOrEquals(31)
                .execute();
        assertFalse(ds.next());
        ds.close();

        // Test less than or equals
        ds = dc.query().from(getDatabaseName()).select("name").and("age").where("age").lessThanOrEquals(31).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[John Doe, 30]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        ds = dc.query().from(getDatabaseName()).select("name").and("age").where("age").lessThanOrEquals(30).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[John Doe, 30]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        ds = dc.query().from(getDatabaseName()).select("name").and("age").where("age").lessThanOrEquals(29).execute();
        assertFalse(ds.next());
        ds.close();

        // test primary key lookup query
        ds = dc.query().from(getDatabaseName()).select("name").and("gender").where("_id").eq(pkValue).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[Jane Doe, F]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        // test primary key null
        ds = dc.query().from(getDatabaseName()).select("name").and("gender").where("_id").isNull().execute();
        assertFalse(ds.next());
        ds.close();

        // test primary key not found
        ds = dc.query().from(getDatabaseName()).select("name").and("gender").where("_id").eq("this id does not exist")
                .execute();
        assertFalse(ds.next());
        ds.close();
    }

    public void testFirstRowAndLastRow() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        // insert a few records
        {
            HashMap<String, Object> map;

            map = new HashMap<String, Object>();
            map.put("name", "John Doe");
            map.put("age", 30);
            connector.create(map);

            map = new HashMap<String, Object>();
            map.put("name", "Jane Doe");
            map.put("gender", 'F');
            connector.create(map);
        }

        // create datacontext using detected schema
        final String databaseName = getDatabaseName();
        final CouchDbDataContext dc = new CouchDbDataContext(couchDbInstance, databaseName);

        final DataSet ds1 = dc.query().from(databaseName).select("name").and("age").firstRow(2).execute();
        final DataSet ds2 = dc.query().from(databaseName).select("name").and("age").maxRows(1).execute();

        assertTrue("Class: " + ds1.getClass().getName(), ds1 instanceof CouchDbDataSet);
        assertTrue("Class: " + ds2.getClass().getName(), ds2 instanceof CouchDbDataSet);

        assertTrue(ds1.next());
        assertTrue(ds2.next());

        final Row row1 = ds1.getRow();
        final Row row2 = ds2.getRow();

        assertFalse(ds1.next());
        assertFalse(ds2.next());

        assertEquals("Row[values=[Jane Doe, null]]", row1.toString());
        assertEquals("Row[values=[John Doe, 30]]", row2.toString());

        ds1.close();
        ds2.close();
    }

    public void testInsert() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        // create datacontext using predefined table def
        CouchDbDataContext dc = new CouchDbDataContext(httpClient, predefinedTableDef);
        Table table = dc.getTableByQualifiedLabel(getDatabaseName());
        assertNotNull(table);

        assertEquals("[_id, _rev, name, gender, age]", Arrays.toString(table.getColumnNames().toArray()));

        DataSet ds;

        // assert not rows in DB
        ds = dc.query().from(getDatabaseName()).selectCount().execute();
        assertTrue(ds.next());
        assertEquals(0, ((Number) ds.getRow().getValue(0)).intValue());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(getDatabaseName()).value("name", "foo").value("gender", 'M').execute();
                callback.insertInto(getDatabaseName()).value("name", "bar").value("age", 32).execute();
            }
        });

        // now count should be 2
        ds = dc.query().from(getDatabaseName()).selectCount().execute();
        assertTrue(ds.next());
        assertEquals(2, ((Number) ds.getRow().getValue(0)).intValue());
        assertFalse(ds.next());
        ds.close();

        ds = dc.query().from(getDatabaseName()).select("name", "gender", "age").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[foo, M, null]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[bar, null, 32]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();
    }
}
