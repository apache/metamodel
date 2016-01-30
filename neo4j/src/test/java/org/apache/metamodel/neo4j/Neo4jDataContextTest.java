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
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.metamodel.DataContext;
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
            final CloseableHttpClient httpClient = HttpClientBuilder.create().build();

            final HttpHost httpHost = new HttpHost(getHostname(), getPort());
            requestWrapper = new Neo4jRequestWrapper(httpClient, httpHost, getUsername(), getPassword(),
                    getServiceRoot());
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

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());
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
        requestWrapper.executeCypherQuery("CREATE (n:JUnitBook { title: 'Rich Dad Poor Dad'})");
        requestWrapper.executeCypherQuery("MATCH (a:JUnitPerson),(b:JUnitBook)"
                + "WHERE a.name = 'Tomasz' AND b.title = 'Introduction to algorithms'"
                + "CREATE (a)-[r:HAS_READ { rating : 5 }]->(b)");
        requestWrapper.executeCypherQuery("MATCH (a:JUnitPerson),(b:JUnitBook)"
                + "WHERE a.name = 'Philomeena' AND b.title = 'Introduction to algorithms'"
                + "CREATE (a)-[r:HAS_BROWSED]->(b)");
        requestWrapper.executeCypherQuery("MATCH (a:JUnitPerson),(b:JUnitBook)"
                + "WHERE a.name = 'Philomeena' AND b.title = 'Introduction to algorithms'"
                + "CREATE (a)-[r:HAS_BROWSED]->(b)");
        requestWrapper.executeCypherQuery("MATCH (a:JUnitPerson),(b:JUnitBook)"
                + "WHERE a.name = 'Tomasz' AND b.title = 'Rich Dad Poor Dad'"
                + "CREATE (a)-[r:HAS_READ { rating : 4 }]->(b)");
        requestWrapper.executeCypherQuery("MATCH (a:JUnitPerson),(b:JUnitBook)"
                + "WHERE a.name = 'Philomeena' AND b.title = 'Rich Dad Poor Dad'"
                + "CREATE (a)-[r:HAS_READ { rating : 2 }]->(b)");

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());
        Schema schema = strategy.getSchemaByName(strategy.getDefaultSchemaName());

        // Do not check the precise count, Neo4j keeps labels forever, there are
        // probably many more than you imagine...
        List<String> tableNames = Arrays.asList(schema.getTableNames());
        logger.info("Tables (labels) detected: " + tableNames);
        assertTrue(tableNames.contains("JUnitPerson"));
        assertTrue(tableNames.contains("JUnitBook"));

        Table tablePerson = schema.getTableByName("JUnitPerson");
        List<String> personColumnNames = Arrays.asList(tablePerson.getColumnNames());
        assertEquals("[_id, name, age, rel_HAS_READ, rel_HAS_READ#rating, rel_HAS_BROWSED]",
                personColumnNames.toString());

        Table tableBook = schema.getTableByName("JUnitBook");
        List<String> bookColumnNames = Arrays.asList(tableBook.getColumnNames());
        assertEquals("[_id, title]", bookColumnNames.toString());
    }

    @Test
    public void testSelectQueryWithProjection() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel { property1: 1, property2: 2 })");

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

        {
            CompiledQuery query = strategy.query().from("JUnitLabel").select("property1").compile();
            try (final DataSet dataSet = strategy.executeQuery(query)) {
                assertTrue(dataSet.next());
                assertEquals("Row[values=[1]]", dataSet.getRow().toString());
                assertFalse(dataSet.next());
            }
        }
        {
            CompiledQuery query = strategy.query().from("JUnitLabel").select("property1").select("property2").compile();
            try (final DataSet dataSet = strategy.executeQuery(query)) {
                assertTrue(dataSet.next());
                assertEquals("Row[values=[1, 2]]", dataSet.getRow().toString());
                assertFalse(dataSet.next());
            }
        }
    }

    public void ignoredTestSelectQueryWithLargeDataset() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        int rowCount = 100000;

        for (int j = 0; j < rowCount / 10000; j++) {
            List<String> cypherQueries = new ArrayList<>();
            for (int i = 0; i < 10000; i++) {
                cypherQueries.add("CREATE (n:JUnitLabel { i: " + (j * 10000 + i) + "})");
            }
            requestWrapper.executeCypherQueries(cypherQueries);
        }
        System.out.println("Inserted " + rowCount + " rows to the database.");

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

        {
            CompiledQuery query = strategy.query().from("JUnitLabel").select("i").orderBy("i").compile();
            try (final DataSet dataSet = strategy.executeQuery(query)) {
                for (int i = 0; i < rowCount; i++) {
                    assertTrue(dataSet.next());
                }
                assertFalse(dataSet.next());
            }
        }
    }

    @Test
    public void testSelectQueryWithProjectionAndRelationships() throws Exception {
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

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

        {
            CompiledQuery query = strategy.query().from("JUnitPerson").select("name", "rel_HAS_READ").compile();
            try (DataSet dataSet = strategy.executeQuery(query)) {
                List<Row> rows = new ArrayList<>();
                while (dataSet.next()) {
                    rows.add(dataSet.getRow());
                }
                // Sorting to have deterministic order
                Collections.sort(rows, new Comparator<Row>() {

                    @Override
                    public int compare(Row arg0, Row arg1) {
                        return arg0.toString().compareTo(arg1.toString());
                    }
                });
                assertEquals(3, rows.size());
                assertEquals("Row[values=[Helena, null]]", rows.get(0).toString());
                assertEquals("Row[values=[Philomeena, null]]", rows.get(1).toString());
                assertEquals("Row[values=[Tomasz, " + bookNodeId + "]]", rows.get(2).toString());
            }
        }
        {
            CompiledQuery query = strategy.query().from("JUnitPerson").select("rel_HAS_READ#rating").compile();
            try (DataSet dataSet = strategy.executeQuery(query)) {
                List<Row> rows = new ArrayList<>();
                while (dataSet.next()) {
                    rows.add(dataSet.getRow());
                }
                // Sorting to have deterministic order
                Collections.sort(rows, new Comparator<Row>() {

                    @Override
                    public int compare(Row arg0, Row arg1) {
                        return arg0.toString().compareTo(arg1.toString());
                    }
                });

                assertEquals(3, rows.size());
                assertEquals("Row[values=[5]]", rows.get(0).toString());
                assertEquals("Row[values=[null]]", rows.get(1).toString());
                assertEquals("Row[values=[null]]", rows.get(2).toString());
            }
        }
    }

    @Test
    public void testSelectAllQueryWithRelationships() throws Exception {
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

        String helenaNodeIdJSONObject = requestWrapper.executeCypherQuery("MATCH (n:JUnitPerson)"
                + " WHERE n.name = 'Helena'" + " RETURN id(n);");
        String helenaNodeId = new JSONObject(helenaNodeIdJSONObject).getJSONArray("results").getJSONObject(0)
                .getJSONArray("data").getJSONObject(0).getJSONArray("row").getString(0);

        String tomaszNodeIdJSONObject = requestWrapper.executeCypherQuery("MATCH (n:JUnitPerson)"
                + " WHERE n.name = 'Tomasz'" + " RETURN id(n);");
        String tomaszNodeId = new JSONObject(tomaszNodeIdJSONObject).getJSONArray("results").getJSONObject(0)
                .getJSONArray("data").getJSONObject(0).getJSONArray("row").getString(0);

        String philomeenaNodeIdJSONObject = requestWrapper.executeCypherQuery("MATCH (n:JUnitPerson)"
                + " WHERE n.name = 'Philomeena'" + " RETURN id(n);");
        String philomeenaNodeId = new JSONObject(philomeenaNodeIdJSONObject).getJSONArray("results").getJSONObject(0)
                .getJSONArray("data").getJSONObject(0).getJSONArray("row").getString(0);

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

        CompiledQuery query = strategy.query().from("JUnitPerson").selectAll().compile();
        try (DataSet dataSet = strategy.executeQuery(query)) {
            List<Row> rows = new ArrayList<>();
            while (dataSet.next()) {
                rows.add(dataSet.getRow());
            }
            // Sorting to have deterministic order
            Collections.sort(rows, new Comparator<Row>() {

                @Override
                public int compare(Row arg0, Row arg1) {
                    return arg0.getValue(1).toString().compareTo(arg1.getValue(1).toString());
                }
            });
            assertEquals(3, rows.size());
            assertEquals("Row[values=[" + helenaNodeId + ", Helena, 100, null, null, null]]", rows.get(0).toString());
            assertEquals("Row[values=[" + philomeenaNodeId + ", Philomeena, 18, null, null, " + bookNodeId + "]]", rows
                    .get(1).toString());
            assertEquals("Row[values=[" + tomaszNodeId + ", Tomasz, 26, " + bookNodeId + ", 5, null]]", rows.get(2)
                    .toString());
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

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());
        {
            CompiledQuery query = strategy.query().from("JUnitLabel").select("property1").where("property2").eq(20)
                    .compile();
            try (final DataSet dataSet = strategy.executeQuery(query)) {
                assertTrue(dataSet.next());
                assertEquals("Row[values=[10]]", dataSet.getRow().toString());
                assertFalse(dataSet.next());
            }
        }
        {
            CompiledQuery query = strategy.query().from("JUnitPerson").select("rel_HAS_READ#rating").where("name")
                    .eq("Tomasz").compile();
            try (final DataSet dataSet = strategy.executeQuery(query)) {
                assertTrue(dataSet.next());
                assertEquals("Row[values=[5]]", dataSet.getRow().toString());
                assertFalse(dataSet.next());
            }
        }
        {
            CompiledQuery query = strategy.query().from("JUnitPerson").select("rel_HAS_READ#rating").where("name")
                    .eq("Philomeena").compile();
            try (final DataSet dataSet = strategy.executeQuery(query)) {
                assertTrue(dataSet.next());
                assertEquals("Row[values=[null]]", dataSet.getRow().toString());
                assertFalse(dataSet.next());
            }
        }
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

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

        Table table1 = strategy.getTableByQualifiedLabel("JUnitLabel1");
        Table table2 = strategy.getTableByQualifiedLabel("JUnitLabel2");
        Column id1Column = table2.getColumnByName("id1");
        Column id2Column = table1.getColumnByName("id2");
        Column propertyTable1Column = table1.getColumnByName("propertyTable1");
        Column propertyTable2Column = table2.getColumnByName("propertyTable2");

        CompiledQuery query = strategy.query().from(table1).and(table2)
                .select(id1Column, id2Column, propertyTable1Column, propertyTable2Column).where(id1Column)
                .eq(id2Column).compile();

        try (final DataSet dataSet = strategy.executeQuery(query)) {
            List<Row> rows = new ArrayList<>();
            while (dataSet.next()) {
                rows.add(dataSet.getRow());
            }
            Collections.sort(rows, new Comparator<Row>() {

                @Override
                public int compare(Row o1, Row o2) {
                    return o1.toString().compareTo(o2.toString());
                }
            });
            assertEquals(2, rows.size());
            assertEquals("Row[values=[1, 1, prop-table1-row1, prop-table2-row1]]", rows.get(0).toString());
            assertEquals("Row[values=[2, 2, prop-table1-row2, prop-table2-row2]]", rows.get(1).toString());
        }
    }

    @Test
    public void testJoinWithRelationships() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

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

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

        Table table1 = strategy.getTableByQualifiedLabel("JUnitPerson");
        Table table2 = strategy.getTableByQualifiedLabel("JUnitBook");
        Column personNameColumn = table1.getColumnByName("name");
        Column personHasReadColumn = table1.getColumnByName("rel_HAS_READ");
        Column bookIdColumn = table2.getColumnByName("_id");
        Column bookTitleColumn = table2.getColumnByName("title");

        CompiledQuery query = strategy.query().from(table1).and(table2)
                .select(personNameColumn, bookIdColumn, bookTitleColumn).where(personHasReadColumn).eq(bookIdColumn)
                .compile();

        try (final DataSet dataSet = strategy.executeQuery(query)) {
            List<Row> rows = new ArrayList<>();
            while (dataSet.next()) {
                rows.add(dataSet.getRow());
            }
            assertEquals(1, rows.size());
            assertEquals("Row[values=[Tomasz, " + bookNodeId + ", Introduction to algorithms]]", rows.get(0).toString());
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
        final DataContext dc = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

        try (final DataSet ds = dc.query().from("JUnitLabel").select("name").and("age").firstRow(2).execute()) {
            assertTrue("Class: " + ds.getClass().getName(), ds instanceof Neo4jDataSet);
            assertTrue(ds.next());
            final Row row = ds.getRow();
            assertEquals("Row[values=[Jane Doe, null]]", row.toString());
            assertFalse(ds.next());
        }

        try (final DataSet ds = dc.query().from("JUnitLabel").select("name").and("age").maxRows(1).execute()) {
            assertTrue("Class: " + ds.getClass().getName(), ds instanceof Neo4jDataSet);
            assertTrue(ds.next());
            final Row row = ds.getRow();
            assertEquals("Row[values=[John Doe, 30]]", row.toString());
            assertFalse(ds.next());
        }
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
        final DataContext dc = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

        try (final DataSet ds = dc.query().from("JUnitLabel").selectCount().where("name").eq("John Doe").execute()) {
            assertTrue(ds.next());
            final Row row = ds.getRow();
            assertFalse(ds.next());
            assertEquals("Row[values=[1]]", row.toString());

        }

        try (final DataSet ds = dc.query().from("JUnitLabel").selectCount().execute()) {
            assertTrue(ds.next());
            final Row row = ds.getRow();
            assertFalse(ds.next());
            assertEquals("Row[values=[2]]", row.toString());
        }
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
