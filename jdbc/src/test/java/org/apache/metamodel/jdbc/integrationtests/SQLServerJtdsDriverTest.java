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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
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
 * "AdventureWorks 2012" sample database which can be downloaded from codeplex.
 * 
 * This testcase uses the JTDS driver.
 * 
 * @link{http://www.codeplex.com/MSFTDBProdSamples
 * */
public class SQLServerJtdsDriverTest extends AbstractJdbIntegrationTest {

    private static final String DATABASE_NAME = "AdventureWorks2012";

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

    // This test is pretty useless. It assumes way too much, and fails due to SQL Server not using timestamp as assumed.
    public void ignoreTestTimestampValueInsertSelect() throws Exception {
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
                Arrays.toString(table.getColumns().toArray()));

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

        Query q = new Query().from(strategy.getTableByQualifiedLabel("Production.Product")).select("Name")
                .where("COlor IS NOT NULL").setMaxRows(5);
        DataSet dataSet = strategy.executeQuery(q);
        assertEquals("[\"Product\".\"Name\"]", Arrays.toString(dataSet.getSelectItems().toArray()));
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

        assertEquals(8, dc.getSchemas().size());

        final Schema hrSchema = dc.getSchemaByName("HumanResources");
        assertNotNull(hrSchema);
        assertEquals(12, hrSchema.getTableCount());

        final Schema informationSchema = dc.getSchemaByName("INFORMATION_SCHEMA");
        assertNotNull(informationSchema);
        assertEquals(21, informationSchema.getTableCount());

        final Schema personSchema = dc.getSchemaByName("Person");
        assertNotNull(personSchema);
        assertEquals(15, personSchema.getTableCount());

        final Schema productionSchema = dc.getSchemaByName("Production");
        assertNotNull(productionSchema);
        assertEquals(28, productionSchema.getTableCount());

        final Schema purchasingSchema = dc.getSchemaByName("Purchasing");
        assertNotNull(purchasingSchema);
        assertEquals(7, purchasingSchema.getTableCount());

        final Schema salesSchema = dc.getSchemaByName("Sales");
        assertNotNull(salesSchema);
        assertEquals(26, salesSchema.getTableCount());
    }

    public void testGetSchemaAllTableTypes() throws Exception {
        if (!isConfigured()) {
            return;
        }
        JdbcDataContext strategy = new JdbcDataContext(getConnection(), new TableType[] { TableType.OTHER,
                TableType.GLOBAL_TEMPORARY }, DATABASE_NAME);
        Schema schema = strategy.getDefaultSchema();
        assertEquals("dbo", schema.getName());

        final List<String> expectedSchemaNames =
                Arrays.asList("Sales", "HumanResources", "dbo", "Purchasing", "sys", "Production", "INFORMATION_SCHEMA",
                        "Person");
        assertTrue(strategy.getSchemaNames().containsAll(expectedSchemaNames));
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

    public void testMaxAndOffset() throws Exception {
        if (!isConfigured()) {
            return;
        }

        final JdbcDataContext context = getDataContext();

        final List<Row> onlyMaxRows =
                context.query().from("Person", "Person").select("BusinessEntityID").maxRows(10).execute().toRows();
        assertEquals("Should limit size even without offset or order by", 10, onlyMaxRows.size());

        final List<Row> onlyOffset =
                context.query().from("Person", "Person").select("BusinessEntityID").orderBy("BusinessEntityID").firstRow(5)
                        .execute().toRows();
        assertEquals("Should offset first row", 5, onlyOffset.get(0).getValue(0));
        assertEquals("Should not limit size beyond offset", 19968, onlyOffset.size());

        final List<Row> maxRowsAndOffset =
                context.query().from("Person", "Person").select("BusinessEntityID").maxRows(20).orderBy("BusinessEntityID")
                        .firstRow(20).execute().toRows();

        assertEquals("Should offset first row", 20, maxRowsAndOffset.get(0).getValue(0));
        assertEquals("Should not limit size", 20, maxRowsAndOffset.size());
    }
}