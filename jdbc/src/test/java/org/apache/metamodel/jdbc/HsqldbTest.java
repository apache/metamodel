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
package org.apache.metamodel.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import javax.swing.table.TableModel;

import junit.framework.TestCase;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetTableModel;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.jdbc.dialects.HsqldbQueryRewriter;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.JoinType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

/**
 * Test case that tests hsqldb interaction. The test uses an embedded copy of
 * the "pentaho sampledata" sample database that can be found at
 * http://pentaho.sourceforge.net.
 */
public class HsqldbTest extends TestCase {

    private static final String CONNECTION_STRING = "jdbc:hsqldb:res:metamodel";
    private static final String USERNAME = "SA";
    private static final String PASSWORD = "";
    private Connection _connection;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Class.forName("org.hsqldb.jdbcDriver");
        _connection = DriverManager.getConnection(CONNECTION_STRING, USERNAME, PASSWORD);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        _connection.close();
    }
    
    public void testApproximateCount() throws Exception {
        final JdbcDataContext dataContext = new JdbcDataContext(_connection);
        final DataSet dataSet = dataContext.executeQuery("SELECT APPROXIMATE COUNT(*) FROM customers");
        assertTrue(dataSet.next());
        assertEquals(122, dataSet.getRow().getValue(0));
        assertFalse(dataSet.next());
    }
    
    public void testTimestampValueInsertSelect() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:" + getName(), USERNAME, PASSWORD);
        JdbcTestTemplates.timestampValueInsertSelect(connection, TimeUnit.NANOSECONDS);
    }

    public void testCreateInsertAndUpdate() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:" + getName(), USERNAME, PASSWORD);
        JdbcDataContext dc = new JdbcDataContext(connection);
        JdbcTestTemplates.simpleCreateInsertUpdateAndDrop(dc, "metamodel_test_simple");
    }

    public void testCompositePrimaryKeyCreation() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:" + getName(), USERNAME, PASSWORD);
        JdbcDataContext dc = new JdbcDataContext(connection);
        JdbcTestTemplates.compositeKeyCreation(dc, "metamodel_test_composite_keys");
    }

    public void testGetSchemas() throws Exception {
        assertNotNull(_connection);
        JdbcDataContext dc = new JdbcDataContext(_connection);
        assertEquals("[Schema[name=INFORMATION_SCHEMA], " + "Schema[name=PUBLIC]]", Arrays.toString(dc.getSchemas()));

        Schema defaultSchema = dc.getDefaultSchema();
        Schema publicSchema = dc.getSchemaByName("PUBLIC");
        assertSame(defaultSchema, publicSchema);
        Table[] tables = publicSchema.getTables();
        assertEquals(13, tables.length);
        assertEquals("[Table[name=CUSTOMERS,type=TABLE,remarks=null], "
                + "Table[name=CUSTOMER_W_TER,type=TABLE,remarks=null], "
                + "Table[name=DEPARTMENT_MANAGERS,type=TABLE,remarks=null], "
                + "Table[name=DIM_TIME,type=TABLE,remarks=null], " + "Table[name=EMPLOYEES,type=TABLE,remarks=null], "
                + "Table[name=OFFICES,type=TABLE,remarks=null], "
                + "Table[name=ORDERDETAILS,type=TABLE,remarks=null], "
                + "Table[name=ORDERFACT,type=TABLE,remarks=null], " + "Table[name=ORDERS,type=TABLE,remarks=null], "
                + "Table[name=PAYMENTS,type=TABLE,remarks=null], " + "Table[name=PRODUCTS,type=TABLE,remarks=null], "
                + "Table[name=QUADRANT_ACTUALS,type=TABLE,remarks=null], "
                + "Table[name=TRIAL_BALANCE,type=TABLE,remarks=null]]", Arrays.toString(tables));

        Table empTable = publicSchema.getTableByName("EMPLOYEES");
        assertEquals(
                "[Column[name=EMPLOYEENUMBER,columnNumber=0,type=INTEGER,nullable=false,nativeType=INTEGER,columnSize=0], "
                        + "Column[name=LASTNAME,columnNumber=1,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=FIRSTNAME,columnNumber=2,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=EXTENSION,columnNumber=3,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=10], "
                        + "Column[name=EMAIL,columnNumber=4,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=100], "
                        + "Column[name=OFFICECODE,columnNumber=5,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=20], "
                        + "Column[name=REPORTSTO,columnNumber=6,type=INTEGER,nullable=true,nativeType=INTEGER,columnSize=0], "
                        + "Column[name=JOBTITLE,columnNumber=7,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50]]",
                Arrays.toString(empTable.getColumns()));

        assertEquals(
                "[Column[name=EMPLOYEENUMBER,columnNumber=0,type=INTEGER,nullable=false,nativeType=INTEGER,columnSize=0]]",
                Arrays.toString(empTable.getPrimaryKeys()));

        // Only a single relationship registered in the database
        assertEquals(
                "[Relationship[primaryTable=PRODUCTS,primaryColumns=[PRODUCTCODE],foreignTable=ORDERFACT,foreignColumns=[PRODUCTCODE]]]",
                Arrays.toString(publicSchema.getRelationships()));
    }

    public void testExecuteQuery() throws Exception {
        JdbcDataContext dc = new JdbcDataContext(_connection);
        Schema schema = dc.getDefaultSchema();
        Table productsTable = schema.getTableByName("PRODUCTS");
        Table factTable = schema.getTableByName("ORDERFACT");

        Query q = new Query().from(new FromItem(JoinType.INNER, productsTable.getRelationships(factTable)[0])).select(
                productsTable.getColumns()[0], factTable.getColumns()[0]);
        assertEquals(
                "SELECT \"PRODUCTS\".\"PRODUCTCODE\", \"ORDERFACT\".\"ORDERNUMBER\" FROM PUBLIC.\"PRODUCTS\" INNER JOIN PUBLIC.\"ORDERFACT\" ON \"PRODUCTS\".\"PRODUCTCODE\" = \"ORDERFACT\".\"PRODUCTCODE\"",
                q.toString());
        assertEquals(25000, dc.getFetchSizeCalculator().getFetchSize(q));

        DataSet data = dc.executeQuery(q);
        TableModel tableModel = new DataSetTableModel(data);
        assertEquals(2, tableModel.getColumnCount());
        assertEquals(2996, tableModel.getRowCount());

        assertEquals(110, MetaModelHelper.executeSingleRowQuery(dc, new Query().selectCount().from(productsTable))
                .getValue(0));
    }

    public void testLimit() throws Exception {
        JdbcDataContext dc = new JdbcDataContext(_connection);
        Schema schema = dc.getDefaultSchema();
        Table productsTable = schema.getTableByName("PRODUCTS");

        DataSet ds = dc.query().from(productsTable).select("PRODUCTCODE").limit(2).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[S10_1678]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[S10_1949]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();
    }

    public void testOffset() throws Exception {
        JdbcDataContext dc = new JdbcDataContext(_connection);
        Schema schema = dc.getDefaultSchema();
        Table productsTable = schema.getTableByName("PRODUCTS");

        DataSet ds = dc.query().from(productsTable).select("PRODUCTCODE").offset(2).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[S10_2016]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[S10_4698]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertTrue(ds.next());
        assertTrue(ds.next());
        ds.close();
    }

    public void testLimitAndOffset() throws Exception {
        JdbcDataContext dc = new JdbcDataContext(_connection);
        Schema schema = dc.getDefaultSchema();
        Table productsTable = schema.getTableByName("PRODUCTS");

        DataSet ds = dc.query().from(productsTable).select("PRODUCTCODE").limit(2).offset(2).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[S10_2016]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[S10_4698]]", ds.getRow().toString());
        assertFalse(ds.next());

        ds.close();
    }

    public void testQueryRewriterQuoteAliases() throws Exception {
        JdbcDataContext dc = new JdbcDataContext(_connection);
        IQueryRewriter queryRewriter = dc.getQueryRewriter();
        assertSame(HsqldbQueryRewriter.class, queryRewriter.getClass());

        Schema schema = dc.getSchemaByName("PUBLIC");
        Table productsTable = schema.getTableByName("PRODUCTS");

        Query q = new Query().from(productsTable, "pro-ducts").select(
                new SelectItem(productsTable.getColumnByName("PRODUCTCODE")).setAlias("c|o|d|e"));
        q.setMaxRows(5);

        assertEquals("SELECT pro-ducts.\"PRODUCTCODE\" AS c|o|d|e FROM PUBLIC.\"PRODUCTS\" pro-ducts", q.toString());

        String queryString = queryRewriter.rewriteQuery(q);
        assertEquals(
                "SELECT TOP 5 \"pro-ducts\".\"PRODUCTCODE\" AS \"c|o|d|e\" FROM PUBLIC.\"PRODUCTS\" \"pro-ducts\"",
                queryString);

        // We have to test that no additional quoting characters are added every
        // time we run the rewriting
        queryString = queryRewriter.rewriteQuery(q);
        queryString = queryRewriter.rewriteQuery(q);
        assertEquals(
                "SELECT TOP 5 \"pro-ducts\".\"PRODUCTCODE\" AS \"c|o|d|e\" FROM PUBLIC.\"PRODUCTS\" \"pro-ducts\"",
                queryString);

        // Test that the original query is still the same (ie. it has been
        // cloned for execution)
        assertEquals("SELECT pro-ducts.\"PRODUCTCODE\" AS c|o|d|e FROM PUBLIC.\"PRODUCTS\" pro-ducts", q.toString());

        DataSet data = dc.executeQuery(q);
        assertNotNull(data);
        data.close();
    }

    public void testQualifiedLabel() throws Exception {
        DataContext dc = new JdbcDataContext(_connection);

        Column column = dc.getDefaultSchema().getTableByName("PRODUCTS").getColumnByName("PRODUCTCODE");
        assertEquals("PUBLIC.PRODUCTS.PRODUCTCODE", column.getQualifiedLabel());
        assertEquals("Table[name=PRODUCTS,type=TABLE,remarks=null]", dc.getTableByQualifiedLabel("PUBLIC.PRODUCTS")
                .toString());
        assertEquals("Table[name=PRODUCTS,type=TABLE,remarks=null]", dc.getTableByQualifiedLabel("PRODUCTS").toString());
        assertEquals(
                "Column[name=PRODUCTCODE,columnNumber=0,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50]",
                dc.getColumnByQualifiedLabel("PUBLIC.PRODUCTS.PRODUCTCODE").toString());
        assertEquals(
                "Column[name=PRODUCTCODE,columnNumber=0,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50]",
                dc.getColumnByQualifiedLabel("PRODUCTS.PRODUCTCODE").toString());
    }

    public void testQuoteInWhereClause() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:quote_in_where", USERNAME, PASSWORD);

        {
            Statement st = connection.createStatement();
            st.executeUpdate("CREATE TABLE testtable (name VARCHAR(255));");
            st.close();
        }

        {
            PreparedStatement st = connection.prepareStatement("INSERT INTO testtable VALUES (?)");
            st.setString(1, "hello");
            st.executeUpdate();

            st.setString(1, "hi");
            st.executeUpdate();

            st.setString(1, "m'jello");
            st.executeUpdate();

            st.close();
        }

        JdbcDataContext dc = new JdbcDataContext(connection);

        Table table = dc.getDefaultSchema().getTableByName("testtable");

        Query q = dc.query().from(table).selectCount().toQuery();
        Row row = MetaModelHelper.executeSingleRowQuery(dc, q);
        assertEquals(3, ((Number) row.getValue(0)).intValue());

        q = dc.query().from(table).selectCount().where("name").isEquals("m'jello").toQuery();

        assertEquals("SELECT COUNT(*) FROM PUBLIC.\"TESTTABLE\" WHERE \"TESTTABLE\".\"NAME\" = 'm'jello'", q.toSql());
        assertEquals("SELECT COUNT(*) FROM PUBLIC.\"TESTTABLE\" WHERE \"TESTTABLE\".\"NAME\" = 'm''jello'", dc
                .getQueryRewriter().rewriteQuery(q));

        row = MetaModelHelper.executeSingleRowQuery(dc, q);
        assertEquals(1, ((Number) row.getValue(0)).intValue());
    }

    public void testWhereColInValues() throws Exception {
        DataContext dc = new JdbcDataContext(_connection);
        DataSet ds = dc.query().from("QUADRANT_ACTUALS").select("REGION").and("DEPARTMENT").where("DEPARTMENT")
                .in("Sales", "Finance").execute();

        assertTrue(ds.next());
        assertEquals("Row[values=[Central, Sales]]", ds.getRow().toString());

        while (ds.next()) {
            Object deptValue = ds.getRow().getValue(1);
            assertTrue("Sales".equals(deptValue) || "Finance".equals(deptValue));
        }
        ds.close();
    }

    public void testAutomaticConversionWhenInsertingString() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:auto_conversion", USERNAME, PASSWORD);

        JdbcTestTemplates.automaticConversionWhenInsertingString(connection);
    }

    public void testWorkingWithDates() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:working_with_dates", USERNAME, PASSWORD);

        final JdbcDataContext dc = new JdbcDataContext(connection);
        final Schema schema = dc.getDefaultSchema();
        JdbcTestTemplates.createInsertAndUpdateDateTypes(dc, schema, "test_table");
    }

    public void testCharOfSizeOn() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:char_of_size_one", USERNAME, PASSWORD);
        JdbcTestTemplates.meaningOfOneSizeChar(connection);
    }

    public void testInsertOfDifferentTypes() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:different_types_insert", USERNAME,
                PASSWORD);

        try {
            connection.createStatement().execute("DROP TABLE my_table");
        } catch (Exception e) {
            // do nothing
        }

        JdbcDataContext dc = new JdbcDataContext(connection);
        final Schema schema = dc.getDefaultSchema();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                Table table = cb.createTable(schema, "my_table").withColumn("id").ofType(ColumnType.INTEGER)
                        .ofNativeType("IDENTITY").nullable(false).withColumn("name").ofType(ColumnType.VARCHAR)
                        .ofSize(10).withColumn("foo").ofType(ColumnType.BOOLEAN).nullable(true).withColumn("bar")
                        .ofType(ColumnType.BOOLEAN).nullable(true).execute();

                assertEquals("MY_TABLE", table.getName());
            }
        });

        try {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback callback) {
                    callback.insertInto("my_table").value("name", "row 1").value("foo", true).execute();

                    callback.insertInto("my_table").value("name", "row 2").value("bar", true).execute();

                    callback.insertInto("my_table").value("name", "row 3").value("foo", true).execute();

                    callback.insertInto("my_table").value("name", "row 4").value("foo", true).execute();

                    callback.insertInto("my_table").value("name", "row 5").value("bar", true).execute();

                    callback.insertInto("my_table").value("name", "row 6").value("foo", true).value("bar", true)
                            .execute();

                    callback.insertInto("my_table").value("name", "row 7").value("foo", true).value("bar", true)
                            .execute();

                    callback.insertInto("my_table").value("name", "row 8").value("foo", false).value("bar", false)
                            .execute();
                }
            });

            DataSet ds = dc.query().from("my_table").select("id").and("name").execute();
            assertTrue(ds.next());
            assertEquals("Row[values=[0, row 1]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[1, row 2]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[2, row 3]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[3, row 4]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[4, row 5]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[5, row 6]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[6, row 7]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[7, row 8]]", ds.getRow().toString());
            assertFalse(ds.next());
            ds.close();
        } finally {
            connection.createStatement().execute("DROP TABLE my_table");
        }
    }

    public void testDifferentOperators() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:hsqldb:mem:different_operators", USERNAME, PASSWORD);
        JdbcTestTemplates.differentOperatorsTest(conn);
    }

    public void testInterpretationOfNull() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:hsqldb:mem:interpretation_of_null", USERNAME, PASSWORD);
        JdbcTestTemplates.interpretationOfNulls(conn);
    }
}