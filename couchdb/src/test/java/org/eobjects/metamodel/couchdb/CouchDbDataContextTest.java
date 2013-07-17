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
package org.eobjects.metamodel.couchdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.ektorp.CouchDbConnector;
import org.ektorp.DbAccessException;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.drop.DropTable;
import org.eobjects.metamodel.insert.InsertInto;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.SimpleTableDef;

public class CouchDbDataContextTest extends TestCase {

    private static final String TEST_DATABASE_NAME = "eobjects_metamodel_test";

    private boolean serverAvailable;

    private HttpClient httpClient;
    private StdCouchDbInstance couchDbInstance;
    private CouchDbConnector connector;
    private SimpleTableDef predefinedTableDef;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        httpClient = new StdHttpClient.Builder().host("localhost").build();

        // set up a simple database
        couchDbInstance = new StdCouchDbInstance(httpClient);

        try {
            if (couchDbInstance.getAllDatabases().contains(TEST_DATABASE_NAME)) {
                throw new IllegalStateException("Couch DB instance already has a database called " + TEST_DATABASE_NAME);
            }
            connector = couchDbInstance.createConnector(TEST_DATABASE_NAME, true);
            System.out.println("Running CouchDB integration tests");
            serverAvailable = true;
        } catch (DbAccessException e) {
            System.out.println("!!! WARNING: Skipping CouchDB tests because local server is not available");
            e.printStackTrace();
            serverAvailable = false;
        }

        final String[] columnNames = new String[] { "name", "gender", "age" };
        final ColumnType[] columnTypes = new ColumnType[] { ColumnType.VARCHAR, ColumnType.CHAR, ColumnType.INTEGER };
        predefinedTableDef = new SimpleTableDef(TEST_DATABASE_NAME, columnNames, columnTypes);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();

        connector = null;

        if (serverAvailable) {
            couchDbInstance.deleteDatabase(TEST_DATABASE_NAME);
        }

        httpClient.shutdown();
    }

    public void testWorkingWithMapsAndLists() throws Exception {
        if (!serverAvailable) {
            return;
        }

        connector = couchDbInstance.createConnector("test_table_map_and_list", true);

        final CouchDbDataContext dc = new CouchDbDataContext(couchDbInstance, new SimpleTableDef("test_table_map_and_list",
                new String[] { "id", "foo", "bar" }, new ColumnType[] { ColumnType.INTEGER, ColumnType.MAP, ColumnType.LIST }));
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

            DataSet ds = dc.query().from(table).select("id","foo","bar").execute();
            assertTrue(ds.next());
            Row row = ds.getRow();
            assertFalse(ds.next());
            ds.close();

            assertEquals(
                    "Row[values=[1, {hello=[world, welt, verden], foo=bar}, [{}, {meta=model, couch=db}]]]",
                    row.toString());
            assertTrue(row.getValue(0) instanceof Integer);
            assertTrue(row.getValue(1) instanceof Map);
            assertTrue(row.getValue(2) instanceof List);

        } finally {
            dc.executeUpdate(new DropTable(table));
        }

    }

    public void testCreateUpdateDeleteScenario() throws Exception {
        if (!serverAvailable) {
            return;
        }

        final CouchDbDataContext dc = new CouchDbDataContext(couchDbInstance);

        // first delete the manually created database!
        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.dropTable(TEST_DATABASE_NAME).execute();
            }
        });

        assertNull(dc.getDefaultSchema().getTableByName(TEST_DATABASE_NAME));

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                Table table = callback.createTable(dc.getDefaultSchema(), TEST_DATABASE_NAME).withColumn("foo")
                        .ofType(ColumnType.VARCHAR).withColumn("greeting").ofType(ColumnType.VARCHAR).execute();
                assertEquals("[_id, _rev, foo, greeting]", Arrays.toString(table.getColumnNames()));
            }
        });

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(TEST_DATABASE_NAME).value("foo", "bar").value("greeting", "hello").execute();
                callback.insertInto(TEST_DATABASE_NAME).value("foo", "baz").value("greeting", "hi").execute();
            }
        });

        DataSet ds = dc.query().from(TEST_DATABASE_NAME).select("_id", "foo", "greeting").execute();
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
                callback.update(TEST_DATABASE_NAME).value("greeting", "howdy").where("foo").isEquals("baz").execute();

                callback.update(TEST_DATABASE_NAME).value("foo", "foo").where("foo").isEquals("bar").execute();
            }
        });

        ds = dc.query().from(TEST_DATABASE_NAME).select("_id", "foo", "greeting").execute();
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
        if (!serverAvailable) {
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
        SimpleTableDef tableDef = CouchDbDataContext.detectTable(connector);
        CouchDbDataContext dc = new CouchDbDataContext(couchDbInstance, tableDef);

        // verify schema and execute query
        Schema schema = dc.getMainSchema();
        assertEquals("[eobjects_metamodel_test]", Arrays.toString(schema.getTableNames()));

        assertEquals("[_id, _rev, age, gender, name]",
                Arrays.toString(schema.getTableByName(TEST_DATABASE_NAME).getColumnNames()));
        Column idColumn = schema.getTableByName(TEST_DATABASE_NAME).getColumnByName("_id");
        assertEquals("Column[name=_id,columnNumber=0,type=VARCHAR,nullable=true,nativeType=null,columnSize=null]",
                idColumn.toString());
        assertTrue(idColumn.isPrimaryKey());

        assertEquals("Column[name=_rev,columnNumber=1,type=VARCHAR,nullable=true,nativeType=null,columnSize=null]", schema
                .getTableByName(TEST_DATABASE_NAME).getColumnByName("_rev").toString());

        DataSet ds;

        ds = dc.query().from(TEST_DATABASE_NAME).select("name").and("age").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[John Doe, 30]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[Jane Doe, null]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        ds = dc.query().from(TEST_DATABASE_NAME).select("name").and("gender").where("age").isNull().execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[Jane Doe, F]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();
    }

    public void testFirstRowAndLastRow() throws Exception {
        if (!serverAvailable) {
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
        SimpleTableDef tableDef = CouchDbDataContext.detectTable(connector);
        CouchDbDataContext dc = new CouchDbDataContext(couchDbInstance, tableDef);

        DataSet ds1 = dc.query().from(TEST_DATABASE_NAME).select("name").and("age").firstRow(2).execute();
        DataSet ds2 = dc.query().from(TEST_DATABASE_NAME).select("name").and("age").maxRows(1).execute();

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
        if (!serverAvailable) {
            return;
        }

        // create datacontext using predefined table def
        CouchDbDataContext dc = new CouchDbDataContext(httpClient, predefinedTableDef);
        Table table = dc.getTableByQualifiedLabel(TEST_DATABASE_NAME);
        assertNotNull(table);

        assertEquals("[_id, _rev, name, gender, age]", Arrays.toString(table.getColumnNames()));

        DataSet ds;

        // assert not rows in DB
        ds = dc.query().from(TEST_DATABASE_NAME).selectCount().execute();
        assertTrue(ds.next());
        assertEquals(0, ((Number) ds.getRow().getValue(0)).intValue());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(TEST_DATABASE_NAME).value("name", "foo").value("gender", 'M').execute();
                callback.insertInto(TEST_DATABASE_NAME).value("name", "bar").value("age", 32).execute();
            }
        });

        // now count should be 2
        ds = dc.query().from(TEST_DATABASE_NAME).selectCount().execute();
        assertTrue(ds.next());
        assertEquals(2, ((Number) ds.getRow().getValue(0)).intValue());
        assertFalse(ds.next());
        ds.close();

        ds = dc.query().from(TEST_DATABASE_NAME).select("name", "gender", "age").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[foo, M, null]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[bar, null, 32]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();
    }
}
