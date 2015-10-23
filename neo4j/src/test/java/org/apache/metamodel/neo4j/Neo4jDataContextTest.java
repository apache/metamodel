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
package org.apache.metamodel.neo4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.http.HttpHost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.CompiledQuery;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jDataContextTest extends Neo4jTestCase {

    private static final Logger logger = LoggerFactory.getLogger(Neo4jDataContextTest.class);

    Neo4jRequestWrapper requestWrapper;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        if (isConfigured()) {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpHost httpHost = new HttpHost(getHostname(), getPort());
            requestWrapper = new Neo4jRequestWrapper(httpClient, httpHost);
        }
    }

    @Test
    public void testTableDetection() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        // Insert a node
        requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel { property1: 1, property2: 2 })");

        // Adding a node, but deleting it afterwards - should not be included in
        // the schema as the table would be empty
        requestWrapper.executeCypherQuery("CREATE (n:JUnitLabelTemp { property1: 3, property2: 4 })");
        requestWrapper.executeCypherQuery("MATCH (n:JUnitLabelTemp) DELETE n;");

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort());
        Schema schema = strategy.getSchemaByName(strategy.getDefaultSchemaName());

        // Do not check the precise count, Neo4j keeps labels forever, there are
        // probably many more than you imagine...
        List<String> tableNames = Arrays.asList(schema.getTableNames());
        logger.info("Tables (labels) detected: " + tableNames);
        assertTrue(tableNames.contains("JUnitLabel"));
        assertFalse(tableNames.contains("JUnitLabelTemp"));

        Table table = schema.getTableByName("JUnitLabel");
        List<String> columnNames = Arrays.asList(table.getColumnNames());
        assertTrue(columnNames.contains("property1"));
        assertTrue(columnNames.contains("property2"));
    }

    @Test
    public void testTableDetectionWithRelationships() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        // Insert nodes
        requestWrapper.executeCypherQuery("CREATE (n:JUnitPerson { name: 'Tomasz', age: 26})");
        requestWrapper.executeCypherQuery("CREATE (n:JUnitPerson { name: 'Philomeena', age: 18})");
        requestWrapper.executeCypherQuery("CREATE (n:JUnitBook { title: 'Introduction to algorithms'})");
        requestWrapper.executeCypherQuery("MATCH (a:JUnitPerson),(b:JUnitBook)"
                + "WHERE a.name = 'Tomasz' AND b.title = 'Introduction to algorithms'"
                + "CREATE (a)-[r:HAS_READ { rating : 5 }]->(b)");
        requestWrapper.executeCypherQuery("MATCH (a:JUnitPerson),(b:JUnitBook)"
                + "WHERE a.name = 'Philomeena' AND b.title = 'Introduction to algorithms'"
                + "CREATE (a)-[r:HAS_BROWSED]->(b)");

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort());
        Schema schema = strategy.getSchemaByName(strategy.getDefaultSchemaName());

        // Do not check the precise count, Neo4j keeps labels forever, there are
        // probably many more than you imagine...
        List<String> tableNames = Arrays.asList(schema.getTableNames());
        logger.info("Tables (labels) detected: " + tableNames);
        assertTrue(tableNames.contains("JUnitPerson"));
        assertTrue(tableNames.contains("JUnitBook"));

        Table tablePerson = schema.getTableByName("JUnitPerson");
        List<String> personColumnNames = Arrays.asList(tablePerson.getColumnNames());
        assertEquals("[name, age, rel_HAS_READ, rel_HAS_READ#rating, rel_HAS_BROWSED]", personColumnNames.toString());

        Table tableBook = schema.getTableByName("JUnitBook");
        List<String> bookColumnNames = Arrays.asList(tableBook.getColumnNames());
        assertEquals("[title]", bookColumnNames.toString());
    }

    @Test
    public void testSelectQuery() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel { property1: 1, property2: 2 })");

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort());

        CompiledQuery query1 = strategy.query().from("JUnitLabel").select("property1").compile();
        DataSet dataSet1 = strategy.executeQuery(query1);
        assertTrue(dataSet1.next());
        assertEquals("Row[values=[1]]", dataSet1.getRow().toString());
        assertFalse(dataSet1.next());

        CompiledQuery query2 = strategy.query().from("JUnitLabel").select("property1").select("property2").compile();
        DataSet dataSet2 = strategy.executeQuery(query2);
        assertTrue(dataSet2.next());
        assertEquals("Row[values=[1, 2]]", dataSet2.getRow().toString());
        assertFalse(dataSet2.next());
    }

    @Test
    public void testSelectQueryWithRelationships() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        // Insert nodes
        requestWrapper.executeCypherQuery("CREATE (n:JUnitPerson { name: 'Tomasz', age: 26})");
        requestWrapper.executeCypherQuery("CREATE (n:JUnitPerson { name: 'Philomeena', age: 18})");
        requestWrapper.executeCypherQuery("CREATE (n:JUnitPerson { name: 'Helena', age: 100})");
        requestWrapper.executeCypherQuery("CREATE (n:JUnitBook { title: 'Introduction to algorithms'})");
        requestWrapper.executeCypherQuery("MATCH (a:JUnitPerson),(b:JUnitBook)"
                + "WHERE a.name = 'Tomasz' AND b.title = 'Introduction to algorithms'"
                + "CREATE (a)-[r:HAS_READ { rating : 5 }]->(b)");
        requestWrapper.executeCypherQuery("MATCH (a:JUnitPerson),(b:JUnitBook)"
                + "WHERE a.name = 'Philomeena' AND b.title = 'Introduction to algorithms'"
                + "CREATE (a)-[r:HAS_BROWSED]->(b)");

        String bookNodeIdJSONObject = requestWrapper.executeCypherQuery("MATCH (n:JUnitBook)"
                + " WHERE n.title = 'Introduction to algorithms'" + " RETURN id(n);");
        String bookNodeId = new JSONObject(bookNodeIdJSONObject).getJSONArray("results").getJSONObject(0)
                .getJSONArray("data").getJSONObject(0).getJSONArray("row").getString(0);

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort());

        CompiledQuery query1 = strategy.query().from("JUnitPerson").select("name", "rel_HAS_READ").compile();
        try (DataSet dataSet1 = strategy.executeQuery(query1)) {
            List<Row> dataSet1Rows = new ArrayList<>();
            while (dataSet1.next()) {
                dataSet1Rows.add(dataSet1.getRow());
            }
            // Sorting to have deterministic order
            Collections.sort(dataSet1Rows, new Comparator<Row>() {

                @Override
                public int compare(Row arg0, Row arg1) {
                    return arg0.toString().compareTo(arg1.toString());
                }
            });
            assertEquals(3, dataSet1Rows.size());
            assertEquals("Row[values=[Helena, null]]", dataSet1Rows.get(0).toString());
            assertEquals("Row[values=[Philomeena, null]]", dataSet1Rows.get(1).toString());
            assertEquals("Row[values=[Tomasz, " + bookNodeId + "]]", dataSet1Rows.get(2).toString());
        }

        // TODO: Test with just a property query and just a relationship query

        CompiledQuery query2 = strategy.query().from("JUnitPerson").select("rel_HAS_READ#rating").compile();
        try (DataSet dataSet2 = strategy.executeQuery(query2)) {
            assertTrue(dataSet2.next());
            assertEquals("Row[values=[5]]", dataSet2.getRow().toString());
            assertFalse(dataSet2.next());
        }
    }

    @Test
    public void testWhereClause() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel { property1: 1, property2: 2 })");
        requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel { property1: 10, property2: 20 })");

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort());

        CompiledQuery query1 = strategy.query().from("JUnitLabel").select("property1").where("property2").eq(20)
                .compile();
        DataSet dataSet1 = strategy.executeQuery(query1);
        assertTrue(dataSet1.next());
        assertEquals("Row[values=[10]]", dataSet1.getRow().toString());
        assertFalse(dataSet1.next());
    }

    @Test
    public void testJoin() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel1 { id2: 1, propertyTable1: \"prop-table1-row1\" })");
        requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel1 { id2: 2, propertyTable1: \"prop-table1-row2\" })");
        requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel2 { id1: 2, propertyTable2: \"prop-table2-row2\" })");
        requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel2 { id1: 1, propertyTable2: \"prop-table2-row1\" })");

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort());

        Table table1 = strategy.getTableByQualifiedLabel("JUnitLabel1");
        Table table2 = strategy.getTableByQualifiedLabel("JUnitLabel2");
        Column id1Column = table2.getColumnByName("id1");
        Column id2Column = table1.getColumnByName("id2");
        Column propertyTable1Column = table1.getColumnByName("propertyTable1");
        Column propertyTable2Column = table2.getColumnByName("propertyTable2");

        CompiledQuery query = strategy.query().from(table1).and(table2)
                .select(id1Column, id2Column, propertyTable1Column, propertyTable2Column).where(id1Column)
                .eq(id2Column).compile();
        DataSet dataSet = null;
        try {
            dataSet = strategy.executeQuery(query);
            assertTrue(dataSet.next());
            assertEquals("Row[values=[1, 1, prop-table1-row1, prop-table2-row1]]", dataSet.getRow().toString());
            assertTrue(dataSet.next());
            assertEquals("Row[values=[2, 2, prop-table1-row2, prop-table2-row2]]", dataSet.getRow().toString());
            assertFalse(dataSet.next());
        } finally {
            if (dataSet != null) {
                dataSet.close();
            }
        }
    }

    @Test
    public void testInsert() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        UpdateableDataContext dataContext = new Neo4jDataContext(getHostname(), getPort());
        try {
            dataContext.executeUpdate(new UpdateScript() {

                @Override
                public void run(UpdateCallback callback) {
                    callback.insertInto("JUnitLabel").value("property1", "updatedValue").execute();
                }
            });
            fail();
        } catch (UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testFirstRowAndLastRow() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        // insert a few records
        {
            requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel { name: 'John Doe', age: 30 })");
            requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel { name: 'Jane Doe', gender: 'F' })");
        }

        // create datacontext using detected schema
        final DataContext dc = new Neo4jDataContext(getHostname(), getPort());

        final DataSet ds1 = dc.query().from("JUnitLabel").select("name").and("age").firstRow(2).execute();
        final DataSet ds2 = dc.query().from("JUnitLabel").select("name").and("age").maxRows(1).execute();

        assertTrue("Class: " + ds1.getClass().getName(), ds1 instanceof Neo4jDataSet);
        assertTrue("Class: " + ds2.getClass().getName(), ds2 instanceof Neo4jDataSet);

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

    @Test
    public void testCountQuery() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        // insert a few records
        {
            requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel { name: 'John Doe', age: 30 })");
            requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel { name: 'Sofia Unknown', gender: 'F' })");
        }

        // create datacontext using detected schema
        final DataContext dc = new Neo4jDataContext(getHostname(), getPort());

        final DataSet ds1 = dc.query().from("JUnitLabel").selectCount().where("name").eq("John Doe").execute();
        final DataSet ds2 = dc.query().from("JUnitLabel").selectCount().execute();

        assertTrue(ds1.next());
        assertTrue(ds2.next());

        final Row row1 = ds1.getRow();
        final Row row2 = ds2.getRow();

        assertFalse(ds1.next());
        assertFalse(ds2.next());

        assertEquals("Row[values=[1]]", row1.toString());
        assertEquals("Row[values=[2]]", row2.toString());

        // TODO: Try-with-resources
        ds1.close();
        ds2.close();
    }

    @Override
    protected void tearDown() throws Exception {
        if (isConfigured()) {
            // Delete the test nodes
            requestWrapper.executeCypherQuery("MATCH (n:JUnitLabel) DELETE n");
            requestWrapper.executeCypherQuery("MATCH (n:JUnitLabelTemp) DELETE n");
            requestWrapper.executeCypherQuery("MATCH (n:JUnitLabel1) DELETE n");
            requestWrapper.executeCypherQuery("MATCH (n:JUnitLabel2) DELETE n");
            requestWrapper.executeCypherQuery("MATCH (n:JUnitPerson)-[r]-() DELETE n,r");
            requestWrapper.executeCypherQuery("MATCH (n:JUnitPerson) DELETE n");
            requestWrapper.executeCypherQuery("MATCH (n:JUnitBook) DELETE n");
        }

        super.tearDown();
    }

}
