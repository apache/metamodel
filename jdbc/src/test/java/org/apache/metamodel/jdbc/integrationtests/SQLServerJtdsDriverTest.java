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
package org.apache.metamodel.jdbc.integrationtests;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.drop.DropTable;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.jdbc.JdbcTestTemplates;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.jdbc.dialects.SQLServerQueryRewriter;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.TableType;

/**
 * Test case that tests MS SQL Server interaction. The test uses the
 * "AdventureWorks" sample database which can be downloaded from codeplex.
 * 
 * This testcase uses the JTDS driver.
 * 
 * @link{http://www.codeplex.com/MSFTDBProdSamples
 * */
public class SQLServerJtdsDriverTest extends AbstractJdbIntegrationTest {

    private static final String DATABASE_NAME = "AdventureWorks";

    @Override
    protected String getPropertyPrefix() {
        return "sqlserver.jtds_driver";
    }

    public void testCreateInsertAndUpdate() throws Exception {
        if (!isConfigured()) {
            return;
        }
        JdbcTestTemplates.simpleCreateInsertUpdateAndDrop(getDataContext(), "metamodel_test_simple");
    }

    public void testTimestampValueInsertSelect() throws Exception {
        if (!isConfigured()) {
            return;
        }

        final Connection connection = getConnection();
        JdbcTestTemplates.timestampValueInsertSelect(connection, TimeUnit.NANOSECONDS, "datetime");
    }

    public void testCreateTableInUpdateScript() throws Exception {
        if (!isConfigured()) {
            return;
        }

        final BasicDataSource dataSource = getDataSource();

        final String tableName = "Pairs";
        final JdbcDataContext dc = new JdbcDataContext(dataSource);
        final Schema schema = dc.getDefaultSchema();

        if (schema.getTableByName(tableName) != null) {
            dc.executeUpdate(new DropTable(schema, tableName));
        }

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                Table table = callback.createTable(schema, tableName).withColumn("GroupID").withColumn("RecordID_1")
                        .withColumn("RecordID_2").withColumn("SimilarityScore").ofType(ColumnType.VARCHAR).execute();
                assertNotNull(table);
            }
        });

        assertNotNull(schema.getTableByName(tableName));

        dc.executeUpdate(new DropTable(schema, tableName));
    }

    public void testCompositePrimaryKeyCreation() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcTestTemplates.compositeKeyCreation(getDataContext(), "metamodel_test_composite_keys");
    }

    public void testWorkingWithDates() throws Exception {
        if (!isConfigured()) {
            return;
        }
        final Connection connection = getConnection();
        assertFalse(connection.isReadOnly());

        JdbcDataContext dc = new JdbcDataContext(connection);
        final Schema schema = dc.getSchemaByName("Person");

        JdbcTestTemplates.createInsertAndUpdateDateTypes(dc, schema, "test_table");
    }

    public void testAutomaticConversionWhenInsertingString() throws Exception {
        if (!isConfigured()) {
            return;
        }
        final Connection connection = getConnection();
        assertNotNull(connection);

        try {
            // clean up, if nescesary
            connection.createStatement().execute("DROP TABLE Person.test_table");
        } catch (SQLException e) {
            // do nothing
        }

        assertFalse(connection.isReadOnly());

        JdbcDataContext dc = new JdbcDataContext(connection);
        final Schema schema = dc.getSchemaByName("Person");
        assertEquals("Person", schema.getName());

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                Table table = cb.createTable(schema, "test_table").withColumn("id").asPrimaryKey()
                        .ofType(ColumnType.INTEGER).withColumn("birthdate").ofType(ColumnType.DATE).execute();

                cb.insertInto(table).value("id", "1").execute();
                cb.insertInto(table).value("id", 2).value("birthdate", "2011-12-21").execute();
            }
        });

        Table table = schema.getTableByName("test_table");

        assertTrue(table.getColumnByName("id").isPrimaryKey());
        assertFalse(table.getColumnByName("birthdate").isPrimaryKey());

        // the jdbc driver represents the date as a VARCHAR
        assertEquals("[Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=int,columnSize=10], "
                + "Column[name=birthdate,columnNumber=1,type=VARCHAR,nullable=true,nativeType=date,columnSize=10]]",
                Arrays.toString(table.getColumns()));

        DataSet ds = dc.query().from(table).select("id").and("birthdate").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1, null]]", ds.getRow().toString());
        assertEquals("java.lang.Integer", ds.getRow().getValue(0).getClass().getName());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, 2011-12-21]]", ds.getRow().toString());
        assertEquals("java.lang.String", ds.getRow().getValue(1).getClass().getName());
        assertFalse(ds.next());
        ds.close();

        connection.createStatement().execute("DROP TABLE Person.test_table");
    }

    public void testQueryUsingExpressions() throws Exception {
        if (!isConfigured()) {
            return;
        }
        JdbcDataContext strategy = new JdbcDataContext(getConnection(), new TableType[] { TableType.TABLE,
                TableType.VIEW }, DATABASE_NAME);
        Query q = new Query().select("Name").from("Production.Product").where("COlor IS NOT NULL").setMaxRows(5);
        DataSet dataSet = strategy.executeQuery(q);
        assertEquals("[Name]", Arrays.toString(dataSet.getSelectItems()));
        assertTrue(dataSet.next());
        assertEquals("Row[values=[LL Crankarm]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertFalse(dataSet.next());
    }

    public void testGetSchemaNormalTableTypes() throws Exception {
        if (!isConfigured()) {
            return;
        }
        JdbcDataContext dc = new JdbcDataContext(getConnection(), new TableType[] { TableType.TABLE, TableType.VIEW },
                DATABASE_NAME);
        Schema[] schemas = dc.getSchemas();

        assertEquals(8, schemas.length);
        assertEquals("Schema[name=HumanResources]", schemas[0].toString());
        assertEquals(13, schemas[0].getTableCount());
        assertEquals("Schema[name=INFORMATION_SCHEMA]", schemas[1].toString());
        assertEquals(20, schemas[1].getTableCount());
        assertEquals("Schema[name=Person]", schemas[2].toString());
        assertEquals(8, schemas[2].getTableCount());
        assertEquals("Schema[name=Production]", schemas[3].toString());
        assertEquals(28, schemas[3].getTableCount());
        assertEquals("Schema[name=Purchasing]", schemas[4].toString());
        assertEquals(8, schemas[4].getTableCount());
        assertEquals("Schema[name=Sales]", schemas[5].toString());
        assertEquals(27, schemas[5].getTableCount());

    }

    public void testGetSchemaAllTableTypes() throws Exception {
        if (!isConfigured()) {
            return;
        }
        JdbcDataContext strategy = new JdbcDataContext(getConnection(), new TableType[] { TableType.OTHER,
                TableType.GLOBAL_TEMPORARY }, DATABASE_NAME);
        Schema schema = strategy.getDefaultSchema();
        assertEquals("dbo", schema.getName());

        assertEquals("[Sales, HumanResources, dbo, Purchasing, sys, Production, INFORMATION_SCHEMA, Person]",
                Arrays.toString(strategy.getSchemaNames()));
    }

    public void testQueryRewriterQuoteAliases() throws Exception {
        if (!isConfigured()) {
            return;
        }
        JdbcDataContext dc = new JdbcDataContext(getConnection(), TableType.DEFAULT_TABLE_TYPES, DATABASE_NAME);
        IQueryRewriter queryRewriter = dc.getQueryRewriter();
        assertSame(SQLServerQueryRewriter.class, queryRewriter.getClass());

        Schema schema = dc.getSchemaByName("Sales");
        Table customersTable = schema.getTableByName("CUSTOMER");

        Query q = new Query().from(customersTable, "cus-tomers").select(
                new SelectItem(customersTable.getColumnByName("AccountNumber")).setAlias("c|o|d|e"));
        q.setMaxRows(5);

        assertEquals("SELECT cus-tomers.\"AccountNumber\" AS c|o|d|e FROM Sales.\"Customer\" cus-tomers", q.toString());

        String queryString = queryRewriter.rewriteQuery(q);
        assertEquals(
                "SELECT TOP 5 \"cus-tomers\".\"AccountNumber\" AS \"c|o|d|e\" FROM Sales.\"Customer\" \"cus-tomers\"",
                queryString);

        // We have to test that no additional quoting characters are added every
        // time we run the rewriting
        queryString = queryRewriter.rewriteQuery(q);
        queryString = queryRewriter.rewriteQuery(q);
        assertEquals(
                "SELECT TOP 5 \"cus-tomers\".\"AccountNumber\" AS \"c|o|d|e\" FROM Sales.\"Customer\" \"cus-tomers\"",
                queryString);

        // Test that the original query is still the same (ie. it has been
        // cloned for execution)
        assertEquals("SELECT cus-tomers.\"AccountNumber\" AS c|o|d|e FROM Sales.\"Customer\" cus-tomers", q.toString());

        DataSet data = dc.executeQuery(q);
        assertNotNull(data);
        data.close();
    }

    public void testQuotedString() throws Exception {
        if (!isConfigured()) {
            return;
        }
        JdbcDataContext dc = new JdbcDataContext(getConnection(), TableType.DEFAULT_TABLE_TYPES, DATABASE_NAME);
        IQueryRewriter queryRewriter = dc.getQueryRewriter();
        assertSame(SQLServerQueryRewriter.class, queryRewriter.getClass());

        Query q = dc.query().from("Production", "Product").select("Name").where("Color").eq("R'ed").toQuery();

        DataSet ds = dc.executeQuery(q);
        assertNotNull(ds);
        assertFalse(ds.next());
        ds.close();

        assertEquals(
                "SELECT Production.\"Product\".\"Name\" FROM Production.\"Product\" WHERE Production.\"Product\".\"Color\" = 'R''ed'",
                queryRewriter.rewriteQuery(q));
    }
}