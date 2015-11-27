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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.jdbc.dialects.DefaultQueryRewriter;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.MutableRef;

/**
 * Test case that tests Derby interaction. The test uses an embedded copy of the
 * "pentaho sampledata" sample database that can be found at
 * http://pentaho.sourceforge.net.
 */
public class DerbyTest extends TestCase {

    private Connection _connection;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        System.setProperty("derby.storage.tempDirector", FileHelper.getTempDir().getAbsolutePath());
        System.setProperty("derby.stream.error.file", File.createTempFile("metamodel-derby", ".log").getAbsolutePath());

        File dbFile = new File("src/test/resources/derby_testdb.jar");
        assertTrue(dbFile.exists());
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        _connection = DriverManager
                .getConnection("jdbc:derby:jar:(" + dbFile.getAbsolutePath() + ")derby_testdb;territory=en");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        _connection.close();

        // clean up the derby.log file
        File logFile = new File("derby.log");
        if (logFile.exists()) {
            logFile.delete();
        }
    }

    public void testTimestampValueInsertSelect() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:derby:target/temp_derby;create=true");
        JdbcTestTemplates.timestampValueInsertSelect(conn, TimeUnit.NANOSECONDS);
    }

    public void testCreateInsertAndUpdate() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:derby:target/temp_derby;create=true");
        JdbcDataContext dc = new JdbcDataContext(conn);
        JdbcTestTemplates.simpleCreateInsertUpdateAndDrop(dc, "metamodel_test_simple");
    }

    public void testCompositePrimaryKeyCreation() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:derby:target/temp_derby;create=true");
        JdbcDataContext dc = new JdbcDataContext(conn);
        JdbcTestTemplates.compositeKeyCreation(dc, "metamodel_test_composite_keys");
    }

    public void testDifferentOperators() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:derby:target/temp_derby;create=true");

        JdbcTestTemplates.differentOperatorsTest(conn);
    }

    public void testTableEqualsBeforeLoading() throws Exception {
        final JdbcDataContext dc1 = new JdbcDataContext(_connection);
        final Table table1 = dc1.getTableByQualifiedLabel("APP.CUSTOMERS");

        final JdbcDataContext dc2 = new JdbcDataContext(_connection);
        final Table table2 = dc2.getTableByQualifiedLabel("APP.CUSTOMERS");

        assertNotSame(table1, table2);
        assertEquals(table1, table2);
    }

    public void testWorkingWithDates() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:derby:target/temp_derby;create=true");

        final JdbcDataContext dc = new JdbcDataContext(conn);
        final Schema schema = dc.getDefaultSchema();
        JdbcTestTemplates.createInsertAndUpdateDateTypes(dc, schema, "test_table");
    }

    public void testCharOfSizeOne() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:derby:target/temp_derby;create=true");
        JdbcTestTemplates.meaningOfOneSizeChar(conn);
    }

    public void testQueryWithFilter() throws Exception {
        JdbcDataContext dc = new JdbcDataContext(_connection, new TableType[] { TableType.TABLE, TableType.VIEW },
                null);
        Query q = dc.query().from("APP", "CUSTOMERS").select("CUSTOMERNUMBER").where("ADDRESSLINE2").isNotNull()
                .toQuery();
        assertEquals(25000, dc.getFetchSizeCalculator().getFetchSize(q));

        q.setMaxRows(5);

        assertEquals(5, dc.getFetchSizeCalculator().getFetchSize(q));

        assertEquals(
                "SELECT \"CUSTOMERS\".\"CUSTOMERNUMBER\" FROM APP.\"CUSTOMERS\" WHERE \"CUSTOMERS\".\"ADDRESSLINE2\" IS NOT NULL",
                q.toSql());

        DataSet dataSet = dc.executeQuery(q);
        assertEquals("[\"CUSTOMERS\".\"CUSTOMERNUMBER\"]", Arrays.toString(dataSet.getSelectItems()));
        assertTrue(dataSet.next());
        assertEquals("Row[values=[114]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertFalse(dataSet.next());
        dataSet.close();
    }

    public void testGetSchemaNormalTableTypes() throws Exception {
        DataContext dc = new JdbcDataContext(_connection, new TableType[] { TableType.TABLE, TableType.VIEW }, null);
        Schema[] schemas = dc.getSchemas();

        assertEquals(11, schemas.length);
        assertEquals("Schema[name=APP]", schemas[0].toString());
        assertEquals(13, schemas[0].getTableCount());
        assertEquals("Schema[name=NULLID]", schemas[1].toString());
        assertEquals(0, schemas[1].getTableCount());
        assertEquals("Schema[name=SQLJ]", schemas[2].toString());
        assertEquals(0, schemas[2].getTableCount());
        assertEquals("Schema[name=SYS]", schemas[3].toString());
        assertEquals(0, schemas[3].getTableCount());
        assertEquals("Schema[name=SYSCAT]", schemas[4].toString());
        assertEquals(0, schemas[4].getTableCount());
        assertEquals("Schema[name=SYSCS_DIAG]", schemas[5].toString());
        assertEquals(0, schemas[5].getTableCount());
        assertEquals("Schema[name=SYSCS_UTIL]", schemas[6].toString());
        assertEquals(0, schemas[6].getTableCount());
        assertEquals("Schema[name=SYSFUN]", schemas[7].toString());
        assertEquals(0, schemas[7].getTableCount());
        assertEquals("Schema[name=SYSIBM]", schemas[8].toString());
        assertEquals(0, schemas[8].getTableCount());
        assertEquals("Schema[name=SYSPROC]", schemas[9].toString());
        assertEquals(0, schemas[9].getTableCount());
        assertEquals("Schema[name=SYSSTAT]", schemas[10].toString());
        assertEquals(0, schemas[10].getTableCount());
    }

    public void testGetSchemaAllTableTypes() throws Exception {
        DataContext dc = new JdbcDataContext(_connection,
                new TableType[] { TableType.OTHER, TableType.GLOBAL_TEMPORARY }, null);
        Schema[] schemas = dc.getSchemas();

        assertEquals(11, schemas.length);
        assertEquals("Schema[name=APP]", schemas[0].toString());
        assertEquals(13, schemas[0].getTableCount());
        assertEquals("[Table[name=CUSTOMERS,type=TABLE,remarks=], " + "Table[name=CUSTOMER_W_TER,type=TABLE,remarks=], "
                + "Table[name=DEPARTMENT_MANAGERS,type=TABLE,remarks=], "
                + "Table[name=EMPLOYEES,type=TABLE,remarks=], " + "Table[name=OFFICES,type=TABLE,remarks=], "
                + "Table[name=ORDERDETAILS,type=TABLE,remarks=], " + "Table[name=ORDERFACT,type=TABLE,remarks=], "
                + "Table[name=ORDERS,type=TABLE,remarks=], " + "Table[name=PAYMENTS,type=TABLE,remarks=], "
                + "Table[name=PRODUCTS,type=TABLE,remarks=], " + "Table[name=QUADRANT_ACTUALS,type=TABLE,remarks=], "
                + "Table[name=TIME,type=TABLE,remarks=], " + "Table[name=TRIAL_BALANCE,type=TABLE,remarks=]]",
                Arrays.toString(schemas[0].getTables()));
        assertEquals("Schema[name=NULLID]", schemas[1].toString());
        assertEquals(0, schemas[1].getTableCount());
        assertEquals("Schema[name=SQLJ]", schemas[2].toString());
        assertEquals(0, schemas[2].getTableCount());
        assertEquals("Schema[name=SYS]", schemas[3].toString());
        assertEquals(18, schemas[3].getTableCount());
        assertEquals("[Table[name=SYSALIASES,type=OTHER,remarks=], " + "Table[name=SYSCHECKS,type=OTHER,remarks=], "
                + "Table[name=SYSCOLPERMS,type=OTHER,remarks=], " + "Table[name=SYSCOLUMNS,type=OTHER,remarks=], "
                + "Table[name=SYSCONGLOMERATES,type=OTHER,remarks=], "
                + "Table[name=SYSCONSTRAINTS,type=OTHER,remarks=], " + "Table[name=SYSDEPENDS,type=OTHER,remarks=], "
                + "Table[name=SYSFILES,type=OTHER,remarks=], " + "Table[name=SYSFOREIGNKEYS,type=OTHER,remarks=], "
                + "Table[name=SYSKEYS,type=OTHER,remarks=], " + "Table[name=SYSROUTINEPERMS,type=OTHER,remarks=], "
                + "Table[name=SYSSCHEMAS,type=OTHER,remarks=], " + "Table[name=SYSSTATEMENTS,type=OTHER,remarks=], "
                + "Table[name=SYSSTATISTICS,type=OTHER,remarks=], " + "Table[name=SYSTABLEPERMS,type=OTHER,remarks=], "
                + "Table[name=SYSTABLES,type=OTHER,remarks=], " + "Table[name=SYSTRIGGERS,type=OTHER,remarks=], "
                + "Table[name=SYSVIEWS,type=OTHER,remarks=]]", Arrays.toString(schemas[3].getTables()));
        assertEquals("Schema[name=SYSCAT]", schemas[4].toString());
        assertEquals(0, schemas[4].getTableCount());
        assertEquals("Schema[name=SYSCS_DIAG]", schemas[5].toString());
        assertEquals(0, schemas[5].getTableCount());
        assertEquals("Schema[name=SYSCS_UTIL]", schemas[6].toString());
        assertEquals(0, schemas[6].getTableCount());
        assertEquals("Schema[name=SYSFUN]", schemas[7].toString());
        assertEquals(0, schemas[7].getTableCount());
        assertEquals("Schema[name=SYSIBM]", schemas[8].toString());
        assertEquals(1, schemas[8].getTableCount());
        assertEquals("[Table[name=SYSDUMMY1,type=OTHER,remarks=]]", Arrays.toString(schemas[8].getTables()));
        assertEquals("Schema[name=SYSPROC]", schemas[9].toString());
        assertEquals(0, schemas[9].getTableCount());
        assertEquals("Schema[name=SYSSTAT]", schemas[10].toString());
        assertEquals(0, schemas[10].getTableCount());

        assertEquals(
                "[Column[name=CUSTOMERNUMBER,columnNumber=0,type=INTEGER,nullable=true,nativeType=INTEGER,columnSize=10], "
                        + "Column[name=CUSTOMERNAME,columnNumber=1,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=255], "
                        + "Column[name=CONTACTLASTNAME,columnNumber=2,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=255], "
                        + "Column[name=CONTACTFIRSTNAME,columnNumber=3,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=255], "
                        + "Column[name=PHONE,columnNumber=4,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=255], "
                        + "Column[name=ADDRESSLINE1,columnNumber=5,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=255], "
                        + "Column[name=ADDRESSLINE2,columnNumber=6,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=255], "
                        + "Column[name=CITY,columnNumber=7,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=255], "
                        + "Column[name=STATE,columnNumber=8,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=255], "
                        + "Column[name=POSTALCODE,columnNumber=9,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=255], "
                        + "Column[name=COUNTRY,columnNumber=10,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=255], "
                        + "Column[name=SALESREPEMPLOYEENUMBER,columnNumber=11,type=INTEGER,nullable=true,nativeType=INTEGER,columnSize=10], "
                        + "Column[name=CREDITLIMIT,columnNumber=12,type=BIGINT,nullable=true,nativeType=BIGINT,columnSize=19]]",
                Arrays.toString(schemas[0].getTableByName("CUSTOMERS").getColumns()));
    }

    public void testQueryRewriterQuoteAliases() throws Exception {
        JdbcDataContext dc = new JdbcDataContext(_connection, new TableType[] { TableType.TABLE, TableType.VIEW },
                null);
        IQueryRewriter queryRewriter = dc.getQueryRewriter();
        assertSame(DefaultQueryRewriter.class, queryRewriter.getClass());

        Schema schema = dc.getSchemaByName("APP");
        Table customersTable = schema.getTableByName("CUSTOMERS");

        Query q = dc.query().from(customersTable).as("cus-tomers").select("CUSTOMERNAME AS c|o|d|e").toQuery();
        assertEquals(25000, dc.getFetchSizeCalculator().getFetchSize(q));

        q.setMaxRows(5);

        assertEquals("SELECT cus-tomers.\"CUSTOMERNAME\" AS c|o|d|e FROM APP.\"CUSTOMERS\" cus-tomers", q.toString());

        String queryString = queryRewriter.rewriteQuery(q);
        assertEquals("SELECT \"cus-tomers\".\"CUSTOMERNAME\" AS \"c|o|d|e\" FROM APP.\"CUSTOMERS\" \"cus-tomers\"",
                queryString);

        // We have to test that no additional quoting characters are added every
        // time we run the rewriting
        queryString = queryRewriter.rewriteQuery(q);
        queryString = queryRewriter.rewriteQuery(q);
        assertEquals("SELECT \"cus-tomers\".\"CUSTOMERNAME\" AS \"c|o|d|e\" FROM APP.\"CUSTOMERS\" \"cus-tomers\"",
                queryString);

        // Test that the original query is still the same (ie. it has been
        // cloned for execution)
        assertEquals("SELECT cus-tomers.\"CUSTOMERNAME\" AS c|o|d|e FROM APP.\"CUSTOMERS\" cus-tomers", q.toString());

        assertEquals(5, dc.getFetchSizeCalculator().getFetchSize(q));

        DataSet data = dc.executeQuery(q);
        assertNotNull(data);
        data.close();
    }

    public void testCreateTable() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:derby:target/temp_derby;create=true");
        assertNotNull(conn);

        try {
            // clean up, if nescesary
            conn.createStatement().execute("DROP TABLE test_table");
        } catch (SQLException e) {
            // do nothing
        }

        assertFalse(conn.isReadOnly());

        JdbcDataContext dc = new JdbcDataContext(conn);
        final Schema schema = dc.getDefaultSchema();
        assertNull(schema.getTableByName("test_table"));

        final MutableRef<Table> writtenTableRef = new MutableRef<Table>();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                JdbcCreateTableBuilder createTableBuilder = (JdbcCreateTableBuilder) cb.createTable(schema,
                        "test_table");
                Table writtenTable = createTableBuilder.withColumn("id").ofType(ColumnType.INTEGER).asPrimaryKey()
                        .withColumn("name").ofSize(255).ofType(ColumnType.VARCHAR).withColumn("age")
                        .ofType(ColumnType.INTEGER).execute();
                writtenTableRef.set(writtenTable);
                String sql = createTableBuilder.createSqlStatement();
                assertEquals(
                        "CREATE TABLE APP.test_table (id INTEGER, name VARCHAR(255), age INTEGER, PRIMARY KEY(id))",
                        sql.replaceAll("\"", "|"));
                assertNotNull(writtenTable);
            }
        });

        try {

            assertSame(writtenTableRef.get(), schema.getTableByName("test_table"));

            assertFalse(conn.isReadOnly());

            dc = new JdbcDataContext(conn);
            assertSame(conn, dc.getConnection());

            final Table readTable = dc.getDefaultSchema().getTableByName("test_table");
            assertEquals("[ID, NAME, AGE]", Arrays.toString(readTable.getColumnNames()));
            assertTrue(readTable.getColumnByName("id").isPrimaryKey());
            assertFalse(readTable.getColumnByName("age").isPrimaryKey());
            assertFalse(readTable.getColumnByName("name").isPrimaryKey());
            assertTrue(writtenTableRef.get().getQualifiedLabel().equalsIgnoreCase(readTable.getQualifiedLabel()));

            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback cb) {
                    cb.insertInto(readTable).value("age", 1).value("name", "hello").value("id", 1).execute();
                    cb.insertInto(readTable).value("name", "world").value("id", 2).execute();
                }
            });

            DataSet ds = dc.query().from(readTable).select(readTable.getColumns()).execute();
            assertTrue(ds.next());
            assertEquals("Row[values=[1, hello, 1]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[2, world, null]]", ds.getRow().toString());
            assertFalse(ds.next());
            ds.close();
        } finally {
            conn.createStatement().execute("DROP TABLE test_table");
        }
    }

    public void testAutomaticConversionWhenInsertingString() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:derby:target/temp_derby;create=true");
        assertNotNull(conn);

        try {
            // clean up, if nescesary
            conn.createStatement().execute("DROP TABLE test_table");
        } catch (SQLException e) {
            // do nothing
        }

        assertFalse(conn.isReadOnly());

        JdbcDataContext dc = new JdbcDataContext(conn);
        final Schema schema = dc.getDefaultSchema();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                Table table = cb.createTable(schema, "test_table").withColumn("id").ofType(ColumnType.INTEGER)
                        .withColumn("birthdate").ofType(ColumnType.DATE).execute();

                cb.insertInto(table).value("id", "1").execute();
                cb.insertInto(table).value("id", 2).value("birthdate", "2011-12-21").execute();
            }
        });

        DataSet ds = dc.query().from("test_table").select("id").and("birthdate").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1, null]]", ds.getRow().toString());
        assertEquals("java.lang.Integer", ds.getRow().getValue(0).getClass().getName());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, 2011-12-21]]", ds.getRow().toString());
        assertEquals("java.sql.Date", ds.getRow().getValue(1).getClass().getName());
        assertFalse(ds.next());
        ds.close();

        conn.createStatement().execute("DROP TABLE test_table");
    }

    public void testConvertClobToString() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:derby:target/temp_derby;create=true");

        JdbcDataContext dc = new JdbcDataContext(conn);

        JdbcTestTemplates.convertClobToString(dc);
    }

    public void testInterpretationOfNull() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:derby:target/temp_derby;create=true");
        JdbcTestTemplates.interpretationOfNulls(conn);
    }
}