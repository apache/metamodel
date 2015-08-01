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

import java.util.Arrays;
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
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
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
    public void testWhereClause() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }
        
        requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel { property1: 1, property2: 2 })");
        requestWrapper.executeCypherQuery("CREATE (n:JUnitLabel { property1: 10, property2: 20 })");

        Neo4jDataContext strategy = new Neo4jDataContext(getHostname(), getPort());

        CompiledQuery query1 = strategy.query().from("JUnitLabel").select("property1").where("property2").eq(20).compile();
        DataSet dataSet1 = strategy.executeQuery(query1);
        assertTrue(dataSet1.next());
        assertEquals("Row[values=[10]]", dataSet1.getRow().toString());
        assertFalse(dataSet1.next());
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

    @Override
    protected void tearDown() throws Exception {
        if (isConfigured()) {
            // Delete the test nodes
            requestWrapper.executeCypherQuery("MATCH (n:JUnitLabel) DELETE n");
            requestWrapper.executeCypherQuery("MATCH (n:JUnitLabelTemp) DELETE n");
        }

        super.tearDown();
    }

}
