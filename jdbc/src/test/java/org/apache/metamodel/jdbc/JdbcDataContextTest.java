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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;
import javax.swing.table.TableModel;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetTableModel;
import org.apache.metamodel.data.EmptyDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.jdbc.dialects.DefaultQueryRewriter;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.query.CompiledQuery;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.OrderByItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.QueryParameter;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.TableType;
import org.easymock.EasyMock;

public class JdbcDataContextTest extends JdbcTestCase {

    public void testGetDefaultSchema() throws Exception {
        Connection con = getTestDbConnection();
        DataContext dc = new JdbcDataContext(con);
        Schema schema = dc.getDefaultSchema();
        assertEquals("PUBLIC", schema.getName());
        con.close();
    }

    public void testGetNonExistingSchema() throws Exception {
        Connection con = getTestDbConnection();
        DataContext dc = new JdbcDataContext(con);
        Schema schema = dc.getSchemaByName("foobar");
        assertNull("found schema: " + schema, schema);
    }

    public void testQueryMaxRows0() throws Exception {
        final Connection con = getTestDbConnection();
        final DataContext dc = new JdbcDataContext(con);
        final Table table = dc.getDefaultSchema().getTables().get(0);
        final DataSet dataSet = dc.query().from(table).selectAll().limit(0).execute();
        assertTrue(dataSet instanceof EmptyDataSet);
        assertFalse(dataSet.next());
        dataSet.close();
    }

    public void testUsingDataSource() throws Exception {
        Connection con = getTestDbConnection();
        DataSource ds = EasyMock.createMock(DataSource.class);

        CloseableConnectionWrapper con1 = new CloseableConnectionWrapper(con);
        CloseableConnectionWrapper con2 = new CloseableConnectionWrapper(con);
        CloseableConnectionWrapper con3 = new CloseableConnectionWrapper(con);
        CloseableConnectionWrapper con4 = new CloseableConnectionWrapper(con);
        CloseableConnectionWrapper con5 = new CloseableConnectionWrapper(con);

        assertFalse(con1.isClosed());
        assertFalse(con2.isClosed());
        assertFalse(con3.isClosed());
        assertFalse(con4.isClosed());
        assertFalse(con5.isClosed());

        EasyMock.expect(ds.getConnection()).andReturn(con1);
        EasyMock.expect(ds.getConnection()).andReturn(con2);
        EasyMock.expect(ds.getConnection()).andReturn(con3);
        EasyMock.expect(ds.getConnection()).andReturn(con4);
        EasyMock.expect(ds.getConnection()).andReturn(con5);

        EasyMock.replay(ds);

        JdbcDataContext dc = new JdbcDataContext(ds);
        dc.refreshSchemas();
        dc.refreshSchemas();

        Schema schema = dc.getDefaultSchema();
        Query q = new Query();
        q.from(schema.getTableByName("CUSTOMERS")).select(new SelectItem("COUNT(*)", null));
        DataSet data = dc.executeQuery(q);
        TableModel tableModel = new DataSetTableModel(data);
        assertEquals(1, tableModel.getRowCount());
        assertEquals(1, tableModel.getColumnCount());
        assertEquals(122, tableModel.getValueAt(0, 0));

        EasyMock.verify(ds);

        String assertionFailMsg = "Expected 5x true: " + con1.isClosed() + "," + con2.isClosed() + ","
                + con3.isClosed() + "," + con4.isClosed() + "," + con5.isClosed();

        assertTrue(assertionFailMsg, con1.isClosed());
        assertTrue(assertionFailMsg, con2.isClosed());
        assertTrue(assertionFailMsg, con3.isClosed());
        assertTrue(assertionFailMsg, con4.isClosed());
        assertTrue(assertionFailMsg, con5.isClosed());
    }

    public void testExecuteQuery() throws Exception {
        Connection connection = getTestDbConnection();
        JdbcDataContext strategy = new JdbcDataContext(connection, new TableType[] { TableType.TABLE, TableType.VIEW },
                null);
        Schema schema = strategy.getSchemaByName(strategy.getDefaultSchemaName());

        Query q = new Query();
        Table table = schema.getTables().get(0);
        q.from(table, "a");
        q.select(table.getColumns());
        assertEquals(
                "SELECT a._CUSTOMERNUMBER_, a._CUSTOMERNAME_, a._CONTACTLASTNAME_, a._CONTACTFIRSTNAME_, a._PHONE_, "
                        + "a._ADDRESSLINE1_, a._ADDRESSLINE2_, a._CITY_, a._STATE_, a._POSTALCODE_, a._COUNTRY_, "
                        + "a._SALESREPEMPLOYEENUMBER_, a._CREDITLIMIT_ FROM PUBLIC._CUSTOMERS_ a", q.toString()
                        .replace('\"', '_'));
        DataSet result = strategy.executeQuery(q);
        assertTrue(result.next());
        assertEquals(
                "Row[values=[103, Atelier graphique, Schmitt, Carine, 40.32.2555, 54, rue Royale, null, Nantes, null, "
                        + "44000, France, 1370, 21000.0]]", result.getRow().toString());
        assertTrue(result.next());
        assertTrue(result.next());
        assertTrue(result.next());
        assertTrue(result.next());
        assertEquals(
                "Row[values=[121, Baane Mini Imports, Bergulfsen, Jonas, 07-98 9555, Erling Skakkes gate 78, null, "
                        + "Stavern, null, 4110, Norway, 1504, 81700.0]]", result.getRow().toString());
        result.close();
    }

    public void testExecuteQueryWithParams() throws Exception {
        Connection connection = getTestDbConnection();
        JdbcDataContext dataContext = new JdbcDataContext(connection,
                new TableType[] { TableType.TABLE, TableType.VIEW }, null);
        Schema schema = dataContext.getSchemaByName(dataContext.getDefaultSchemaName());

        QueryParameter queryParameter = new QueryParameter();

        Query q = new Query();
        Table table = schema.getTables().get(0);
        q.select(table.getColumns());
        q.from(table, "a");
        q.where(table.getColumnByName("CUSTOMERNUMBER"), OperatorType.EQUALS_TO, queryParameter);
        q.where(table.getColumnByName("CUSTOMERNAME"), OperatorType.EQUALS_TO, queryParameter);

        final CompiledQuery compiledQuery = dataContext.compileQuery(q);

        final JdbcCompiledQuery jdbcCompiledQuery = (JdbcCompiledQuery) compiledQuery;
        assertEquals(0, jdbcCompiledQuery.getActiveLeases());
        assertEquals(0, jdbcCompiledQuery.getIdleLeases());

        String compliedQueryString = compiledQuery.toSql();

        assertEquals(
                "SELECT a._CUSTOMERNUMBER_, a._CUSTOMERNAME_, a._CONTACTLASTNAME_, a._CONTACTFIRSTNAME_, a._PHONE_, "
                        + "a._ADDRESSLINE1_, a._ADDRESSLINE2_, a._CITY_, "
                        + "a._STATE_, a._POSTALCODE_, a._COUNTRY_, a._SALESREPEMPLOYEENUMBER_, "
                        + "a._CREDITLIMIT_ FROM PUBLIC._CUSTOMERS_ a WHERE a._CUSTOMERNUMBER_ = ? "
                        + "AND a._CUSTOMERNAME_ = ?", compliedQueryString.replace('\"', '_'));
        DataSet result1 = dataContext.executeQuery(compiledQuery, new Object[] { 103, "Atelier graphique" });
        assertTrue(result1.next());
        assertEquals(
                "Row[values=[103, Atelier graphique, Schmitt, Carine, 40.32.2555, 54, rue Royale, null, Nantes, null, "
                        + "44000, France, 1370, 21000.0]]", result1.getRow().toString());
        assertFalse(result1.next());

        assertEquals(1, jdbcCompiledQuery.getActiveLeases());
        assertEquals(0, jdbcCompiledQuery.getIdleLeases());

        DataSet result2 = dataContext.executeQuery(compiledQuery, new Object[] { 103, "Atelier graphique" });

        assertEquals(2, jdbcCompiledQuery.getActiveLeases());
        assertEquals(0, jdbcCompiledQuery.getIdleLeases());

        result1.close();

        assertEquals(1, jdbcCompiledQuery.getActiveLeases());
        assertEquals(1, jdbcCompiledQuery.getIdleLeases());

        result2.close();

        assertEquals(0, jdbcCompiledQuery.getActiveLeases());
        assertEquals(2, jdbcCompiledQuery.getIdleLeases());

        compiledQuery.close();

        assertEquals(0, jdbcCompiledQuery.getActiveLeases());
        assertEquals(0, jdbcCompiledQuery.getIdleLeases());
    }

    public void testSelectScalarFunction() throws Exception {
        final Connection connection = getTestDbConnection();
        final JdbcDataContext dataContext = new JdbcDataContext(connection);
        final DataSet dataSet = dataContext.query().from("customers").select("creditlimit")
                .select(FunctionType.TO_DATE, "creditlimit").select("creditlimit").limit(2).execute();
        try {
            assertEquals("[_CUSTOMERS_._CREDITLIMIT_, TO_DATE(_CUSTOMERS_._CREDITLIMIT_), _CUSTOMERS_._CREDITLIMIT_]",
                    Arrays.toString(dataSet.getSelectItems().toArray()).replaceAll("\"", "_"));

            assertTrue(dataSet.next());
            final Object value0 = dataSet.getRow().getValue(0);
            assertTrue("Expected a number but got: " + value0, value0 instanceof Number);
            final Object value1 = dataSet.getRow().getValue(1);
            assertTrue("Expected a date but got: " + value1, value1 instanceof Date);
            final Object value2 = dataSet.getRow().getValue(2);
            assertTrue("Expected a number but got: " + value2, value2 instanceof Number);
        } finally {
            dataSet.close();
        }
    }

    public void testWhereScalarFunction() throws Exception {
        final Connection connection = getTestDbConnection();
        final JdbcDataContext dataContext = new JdbcDataContext(connection);
        try {
            dataContext.query().from("customers").select("customernumber")
                    .where(FunctionType.TO_BOOLEAN, "creditlimit").eq(true).limit(2).execute();
            fail("Exception expected");
        } catch (MetaModelException e) {
            assertEquals(
                    "Scalar functions outside of SELECT clause is not supported for JDBC databases. Query rejected: "
                            + "SELECT \"CUSTOMERS\".\"CUSTOMERNUMBER\" FROM PUBLIC.\"CUSTOMERS\" WHERE TO_BOOLEAN(\"CUSTOMERS\".\"CREDITLIMIT\") = TRUE",
                    e.getMessage());
        }
    }

    public void testUnsupportedAggregateFunction() throws Exception {
        final Connection connection = getTestDbConnection();
        final JdbcDataContext dataContext = new JdbcDataContext(connection);
        try {
            dataContext.query().from("customers").select(FunctionType.RANDOM, "customernumber").execute();
            fail("Exception expected");
        } catch (MetaModelException e) {
            assertEquals("Aggregate function 'RANDOM' is not supported on this JDBC database. Query rejected: "
                    + "SELECT RANDOM(\"CUSTOMERS\".\"CUSTOMERNUMBER\") FROM PUBLIC.\"CUSTOMERS\"", e.getMessage());
        }
    }

    public void testExecuteQueryWithComparisonGreaterThanOrEquals() throws Exception {
        Connection connection = getTestDbConnection();
        JdbcDataContext dataContext = new JdbcDataContext(connection,
                new TableType[] { TableType.TABLE, TableType.VIEW }, null);
        Schema schema = dataContext.getSchemaByName(dataContext.getDefaultSchemaName());

        QueryParameter queryParameter = new QueryParameter();

        Query q = new Query();
        Table table = schema.getTables().get(0);
        q.select(new SelectItem("COUNT(*)", null));
        q.from(table, "a");
        q.where(table.getColumnByName("CREDITLIMIT"), OperatorType.GREATER_THAN_OR_EQUAL, queryParameter);

        final CompiledQuery compiledQuery = dataContext.compileQuery(q);

        final JdbcCompiledQuery jdbcCompiledQuery = (JdbcCompiledQuery) compiledQuery;
        assertEquals(0, jdbcCompiledQuery.getActiveLeases());
        assertEquals(0, jdbcCompiledQuery.getIdleLeases());

        String compliedQueryString = compiledQuery.toSql();

        assertEquals("SELECT COUNT(*) FROM PUBLIC._CUSTOMERS_ a WHERE a._CREDITLIMIT_ >= ?",
                compliedQueryString.replace('\"', '_'));

        DataSet result1 = dataContext.executeQuery(compiledQuery, new Object[] { 11000 });
        assertTrue(result1.next());
        assertEquals("Row[values=[98]]", result1.getRow().toString());
        assertFalse(result1.next());
        result1.close();

        result1 = dataContext.executeQuery(compiledQuery, new Object[] { 10999 });
        assertTrue(result1.next());
        assertEquals("Row[values=[98]]", result1.getRow().toString());
        assertFalse(result1.next());
        result1.close();

        result1 = dataContext.executeQuery(compiledQuery, new Object[] { 11001 });
        assertTrue(result1.next());
        assertEquals("Row[values=[97]]", result1.getRow().toString());
        assertFalse(result1.next());

        assertEquals(1, jdbcCompiledQuery.getActiveLeases());
        assertEquals(0, jdbcCompiledQuery.getIdleLeases());

        result1.close();

        assertEquals(0, jdbcCompiledQuery.getActiveLeases());
        assertEquals(1, jdbcCompiledQuery.getIdleLeases());

        compiledQuery.close();

        assertEquals(0, jdbcCompiledQuery.getActiveLeases());
        assertEquals(0, jdbcCompiledQuery.getIdleLeases());
    }

    public void testExecuteQueryWithComparisonLessThanOrEquals() throws Exception {
        Connection connection = getTestDbConnection();
        JdbcDataContext dataContext = new JdbcDataContext(connection,
                new TableType[] { TableType.TABLE, TableType.VIEW }, null);
        Schema schema = dataContext.getSchemaByName(dataContext.getDefaultSchemaName());

        QueryParameter queryParameter = new QueryParameter();

        Query q = new Query();
        Table table = schema.getTables().get(0);
        q.select(new SelectItem("COUNT(*)", null));
        q.from(table, "a");
        q.where(table.getColumnByName("CREDITLIMIT"), OperatorType.LESS_THAN_OR_EQUAL, queryParameter);

        final CompiledQuery compiledQuery = dataContext.compileQuery(q);

        final JdbcCompiledQuery jdbcCompiledQuery = (JdbcCompiledQuery) compiledQuery;
        assertEquals(0, jdbcCompiledQuery.getActiveLeases());
        assertEquals(0, jdbcCompiledQuery.getIdleLeases());

        String compliedQueryString = compiledQuery.toSql();

        assertEquals("SELECT COUNT(*) FROM PUBLIC._CUSTOMERS_ a WHERE a._CREDITLIMIT_ <= ?",
                compliedQueryString.replace('\"', '_'));

        DataSet result1 = dataContext.executeQuery(compiledQuery, new Object[] { 11000 });
        assertTrue(result1.next());
        assertEquals("Row[values=[25]]", result1.getRow().toString());
        assertFalse(result1.next());
        result1.close();

        result1 = dataContext.executeQuery(compiledQuery, new Object[] { 11001 });
        assertTrue(result1.next());
        assertEquals("Row[values=[25]]", result1.getRow().toString());
        assertFalse(result1.next());
        result1.close();

        result1 = dataContext.executeQuery(compiledQuery, new Object[] { 10999 });
        assertTrue(result1.next());
        assertEquals("Row[values=[24]]", result1.getRow().toString());
        assertFalse(result1.next());

        assertEquals(1, jdbcCompiledQuery.getActiveLeases());
        assertEquals(0, jdbcCompiledQuery.getIdleLeases());

        result1.close();

        assertEquals(0, jdbcCompiledQuery.getActiveLeases());
        assertEquals(1, jdbcCompiledQuery.getIdleLeases());

        compiledQuery.close();

        assertEquals(0, jdbcCompiledQuery.getActiveLeases());
        assertEquals(0, jdbcCompiledQuery.getIdleLeases());
    }

    public void testGetSchemaNormalTableTypes() throws Exception {
        Connection connection = getTestDbConnection();
        JdbcDataContext dc = new JdbcDataContext(connection, new TableType[] { TableType.TABLE, TableType.VIEW }, null);
        Schema[] schemas = dc.getSchemas().toArray(new Schema[dc.getSchemas().size()]);

        assertEquals(2, schemas.length);
        assertEquals("Schema[name=INFORMATION_SCHEMA]", schemas[0].toString());
        assertEquals(0, schemas[0].getTableCount());
        assertEquals("Schema[name=PUBLIC]", schemas[1].toString());
        assertEquals(13, schemas[1].getTableCount());

        connection.close();
    }

    public void testParseColumns() throws Exception {
        Connection connection = getTestDbConnection();
        JdbcDataContext dc = new JdbcDataContext(connection, new TableType[] { TableType.OTHER,
                TableType.GLOBAL_TEMPORARY }, null);
        Schema schema = dc.getDefaultSchema();
        Table customersTable = schema.getTableByName("CUSTOMERS");
        assertEquals(
                "[Column[name=CUSTOMERNUMBER,columnNumber=0,type=INTEGER,nullable=false,nativeType=INTEGER,columnSize=0], "
                        + "Column[name=CUSTOMERNAME,columnNumber=1,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=CONTACTLASTNAME,columnNumber=2,type=VARCHAR,nullable=false,nativeType=VARCHAR,"
                        + "columnSize=50], "
                        + "Column[name=CONTACTFIRSTNAME,columnNumber=3,type=VARCHAR,nullable=false,nativeType=VARCHAR,"
                        + "columnSize=50], "
                        + "Column[name=PHONE,columnNumber=4,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=ADDRESSLINE1,columnNumber=5,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=ADDRESSLINE2,columnNumber=6,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=CITY,columnNumber=7,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=STATE,columnNumber=8,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=POSTALCODE,columnNumber=9,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=15], "
                        + "Column[name=COUNTRY,columnNumber=10,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=SALESREPEMPLOYEENUMBER,columnNumber=11,type=INTEGER,nullable=true,nativeType=INTEGER,"
                        + "columnSize=0], "
                        + "Column[name=CREDITLIMIT,columnNumber=12,type=NUMERIC,nullable=true,nativeType=NUMERIC,columnSize=17]]",
                Arrays.toString(customersTable.getColumns().toArray()));
        connection.close();
    }

    public void testParseRelations() throws Exception {
        Connection connection = getTestDbConnection();
        JdbcDataContext dc = new JdbcDataContext(connection, new TableType[] { TableType.TABLE }, null);
        Schema schema = dc.getDefaultSchema();
        Table productsTable = schema.getTableByName("PRODUCTS");

        /**
         * TODO: A single constraint now exists, create more ...
         */
        assertEquals("[Relationship[primaryTable=PRODUCTS,primaryColumns=[PRODUCTCODE],foreignTable=ORDERFACT,"
                + "foreignColumns=[PRODUCTCODE]]]", Arrays.toString(productsTable.getRelationships().toArray()));

        assertEquals(
                "[Column[name=PRODUCTCODE,columnNumber=0,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50]]",
                Arrays.toString(productsTable.getIndexedColumns().toArray()));

        connection.close();
    }

    public void testGetCatalogNames() throws Exception {
        Connection connection = getTestDbConnection();
        JdbcDataContext strategy = new JdbcDataContext(connection);
        String[] catalogNames = strategy.getCatalogNames();
        assertEquals(0, catalogNames.length);
    }

    public void testMaxRows() throws Exception {
        final Connection realCon = getTestDbConnection();
        final Statement realStatement = realCon.createStatement();

        final Connection mockCon = EasyMock.createMock(Connection.class);
        final Statement mockStatement = EasyMock.createMock(Statement.class);

        EasyMock.expect(mockCon.getMetaData()).andReturn(realCon.getMetaData()).anyTimes();

        EasyMock.expect(mockCon.getAutoCommit()).andReturn(true);

        EasyMock.expect(mockCon.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)).andReturn(
                mockStatement);

        EasyMock.expect(mockStatement.getFetchSize()).andReturn(10);
        mockStatement.setFetchSize(EasyMock.anyInt());
        mockStatement.setMaxRows(3);
        EasyMock.expectLastCall().andThrow(new SQLException("I wont allow max rows"));

        EasyMock.expect(
                mockStatement
                        .executeQuery("SELECT a.\"CUSTOMERNUMBER\", a.\"CUSTOMERNAME\", a.\"CONTACTLASTNAME\", a.\"CONTACTFIRSTNAME\", "
                                + "a.\"PHONE\", a.\"ADDRESSLINE1\", a.\"ADDRESSLINE2\", a.\"CITY\", a.\"STATE\", "
                                + "a.\"POSTALCODE\", a.\"COUNTRY\", a.\"SALESREPEMPLOYEENUMBER\", "
                                + "a.\"CREDITLIMIT\" FROM PUBLIC.\"CUSTOMERS\" a")).andReturn(
                realStatement.executeQuery("SELECT a.\"CUSTOMERNUMBER\", a.\"CUSTOMERNAME\", a.\"CONTACTLASTNAME\", "
                        + "a.\"CONTACTFIRSTNAME\", a.\"PHONE\", a.\"ADDRESSLINE1\", a.\"ADDRESSLINE2\", a.\"CITY\", "
                        + "a.\"STATE\", a.\"POSTALCODE\", a.\"COUNTRY\", a.\"SALESREPEMPLOYEENUMBER\", "
                        + "a.\"CREDITLIMIT\" FROM PUBLIC.\"CUSTOMERS\" a"));

        mockStatement.close();

        EasyMock.replay(mockCon, mockStatement);

        JdbcDataContext dc = new JdbcDataContext(mockCon, new TableType[] { TableType.TABLE, TableType.VIEW }, null);
        dc.setQueryRewriter(new DefaultQueryRewriter(dc));
        Schema schema = dc.getDefaultSchema();

        Query q = new Query().setMaxRows(3);
        Table table = schema.getTables().get(0);
        q.from(table, "a");
        q.select(table.getColumns());
        assertEquals("SELECT a.\"CUSTOMERNUMBER\", a.\"CUSTOMERNAME\", a.\"CONTACTLASTNAME\", a.\"CONTACTFIRSTNAME\", "
                + "a.\"PHONE\", a.\"ADDRESSLINE1\", a.\"ADDRESSLINE2\", a.\"CITY\", a.\"STATE\", a.\"POSTALCODE\", "
                + "a.\"COUNTRY\", a.\"SALESREPEMPLOYEENUMBER\", a.\"CREDITLIMIT\" FROM PUBLIC.\"CUSTOMERS\" a",
                q.toString());
        DataSet result = dc.executeQuery(q);
        assertTrue(result.next());
        assertEquals(
                "Row[values=[103, Atelier graphique, Schmitt, Carine, 40.32.2555, 54, rue Royale, null, Nantes, null, "
                        + "44000, France, 1370, 21000.0]]", result.getRow().toString());
        assertTrue(result.next());
        assertTrue(result.next());
        assertFalse(result.next());

        result.close();

        EasyMock.verify(mockCon, mockStatement);
        realStatement.close();
    }

    public void testMaxRowsRewrite() throws Exception {
        JdbcDataContext dc = new JdbcDataContext(getTestDbConnection(), new TableType[] { TableType.TABLE }, null);
        IQueryRewriter rewriter = new DefaultQueryRewriter(dc) {
            @Override
            public String rewriteQuery(Query query) {
                return "SELECT COUNT(*) FROM PUBLIC.CUSTOMERS";
            }
        };
        dc = dc.setQueryRewriter(rewriter);
        TableModel tm = new DataSetTableModel(dc.executeQuery(new Query().selectCount()));
        assertEquals(1, tm.getRowCount());
        assertEquals(1, tm.getColumnCount());
        assertEquals("122", tm.getValueAt(0, 0).toString());
    }

    /**
     * Executes the same query on two diffent strategies, one with database-side
     * query execution and one with Query postprocessing
     */
    public void testCsvQueryResultComparison() throws Exception {
        Connection connection = getTestDbConnection();
        final DataContext dataContext1 = new JdbcDataContext(connection);
        final DataContext dataContext2 = new QueryPostprocessDataContext() {
            @Override
            public DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
                Query q = new Query();
                q.from(table, "a");
                q.select(columns);
                return dataContext1.executeQuery(q);
            }

            @Override
            protected Schema getMainSchema() throws MetaModelException {
                throw new UnsupportedOperationException();
            }

            @Override
            protected String getMainSchemaName() throws MetaModelException {
                return "PUBLIC";
            }
        };

        Schema schema2 = dataContext1.getDefaultSchema();
        Table customersTable = schema2.getTableByName("CUSTOMERS");
        Table employeeTable = schema2.getTableByName("EMPLOYEES");
        assertEquals(
                "[Column[name=CUSTOMERNUMBER,columnNumber=0,type=INTEGER,nullable=false,nativeType=INTEGER,columnSize=0], "
                        + "Column[name=CUSTOMERNAME,columnNumber=1,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=CONTACTLASTNAME,columnNumber=2,type=VARCHAR,nullable=false,nativeType=VARCHAR,"
                        + "columnSize=50], "
                        + "Column[name=CONTACTFIRSTNAME,columnNumber=3,type=VARCHAR,nullable=false,nativeType=VARCHAR,"
                        + "columnSize=50], "
                        + "Column[name=PHONE,columnNumber=4,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=ADDRESSLINE1,columnNumber=5,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=ADDRESSLINE2,columnNumber=6,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=CITY,columnNumber=7,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=STATE,columnNumber=8,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=POSTALCODE,columnNumber=9,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=15], "
                        + "Column[name=COUNTRY,columnNumber=10,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=SALESREPEMPLOYEENUMBER,columnNumber=11,type=INTEGER,nullable=true,nativeType=INTEGER,"
                        + "columnSize=0], "
                        + "Column[name=CREDITLIMIT,columnNumber=12,type=NUMERIC,nullable=true,nativeType=NUMERIC,columnSize=17]]",
                Arrays.toString(customersTable.getColumns().toArray()));
        assertEquals(
                "[Column[name=EMPLOYEENUMBER,columnNumber=0,type=INTEGER,nullable=false,nativeType=INTEGER,columnSize=0], "
                        + "Column[name=LASTNAME,columnNumber=1,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=FIRSTNAME,columnNumber=2,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
                        + "Column[name=EXTENSION,columnNumber=3,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=10], "
                        + "Column[name=EMAIL,columnNumber=4,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=100], "
                        + "Column[name=OFFICECODE,columnNumber=5,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=20], "
                        + "Column[name=REPORTSTO,columnNumber=6,type=INTEGER,nullable=true,nativeType=INTEGER,columnSize=0], "
                        + "Column[name=JOBTITLE,columnNumber=7,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50]]",
                Arrays.toString(employeeTable.getColumns().toArray()));

        Column employeeNumberColumn1 = customersTable.getColumnByName("SALESREPEMPLOYEENUMBER");
        Column countryColumn = customersTable.getColumnByName("COUNTRY");
        Column employeeNumberColumn2 = employeeTable.getColumnByName("EMPLOYEENUMBER");
        Column creditLimitColumn = customersTable.getColumnByName("CREDITLIMIT");

        Query q = new Query();
        q.from(customersTable, "c");
        q.from(employeeTable, "o");
        SelectItem countrySelect = new SelectItem(countryColumn);
        q.select(countrySelect, new SelectItem(FunctionType.SUM, creditLimitColumn));
        q.groupBy(countryColumn);
        q.orderBy(new OrderByItem(countrySelect));
        q.where(new FilterItem(new SelectItem(employeeNumberColumn1), OperatorType.EQUALS_TO, new SelectItem(
                employeeNumberColumn2)));

        assertEquals("SELECT c.\"COUNTRY\", SUM(c.\"CREDITLIMIT\") FROM PUBLIC.\"CUSTOMERS\" c, "
                + "PUBLIC.\"EMPLOYEES\" o WHERE c.\"SALESREPEMPLOYEENUMBER\" = o.\"EMPLOYEENUMBER\" GROUP BY c"
                + ".\"COUNTRY\" ORDER BY c.\"COUNTRY\" ASC", q.toString());

        DataSet data1 = dataContext1.executeQuery(q);
        assertTrue(data1.next());
        assertEquals("Row[values=[Australia, 430300.0]]", data1.getRow().toString());
        DataSet data2 = dataContext2.executeQuery(q);
        assertTrue(data2.next());
        assertEquals("Row[values=[Australia, 430300.0]]", data2.getRow().toString());
        assertEquals(new DataSetTableModel(data1), new DataSetTableModel(data2));
    }

    public void testReleaseConnectionsInCompiledQuery() throws Exception {
        final int connectionPoolSize = 2;
        final int threadCount = 4;
        final int noOfCallsPerThreads = 30;

        final BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.hsqldb.jdbcDriver");
        ds.setUrl("jdbc:hsqldb:res:metamodel");
        ds.setInitialSize(connectionPoolSize);
        ds.setMaxActive(connectionPoolSize);
        ds.setMaxWait(10000);
        ds.setMinEvictableIdleTimeMillis(1800000);
        ds.setMinIdle(0);
        ds.setMaxIdle(connectionPoolSize);
        ds.setNumTestsPerEvictionRun(3);
        ds.setTimeBetweenEvictionRunsMillis(-1);
        ds.setDefaultTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);

        final JdbcDataContext dataContext = new JdbcDataContext(ds,
                new TableType[] { TableType.TABLE, TableType.VIEW }, null);

        final JdbcCompiledQuery compiledQuery = (JdbcCompiledQuery) dataContext.query().from("CUSTOMERS")
                .select("CUSTOMERNAME").where("CUSTOMERNUMBER").eq(new QueryParameter()).compile();

        assertEquals(0, compiledQuery.getActiveLeases());
        assertEquals(0, compiledQuery.getIdleLeases());

        final String compliedQueryString = compiledQuery.toSql();

        assertEquals(
                "SELECT _CUSTOMERS_._CUSTOMERNAME_ FROM PUBLIC._CUSTOMERS_ WHERE _CUSTOMERS_._CUSTOMERNUMBER_ = ?",
                compliedQueryString.replace('\"', '_'));

        assertEquals(0, compiledQuery.getActiveLeases());
        assertEquals(0, compiledQuery.getIdleLeases());

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final List<Throwable> errors = new ArrayList<Throwable>();
        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < noOfCallsPerThreads; i++) {
                        final DataSet dataSet = dataContext.executeQuery(compiledQuery, new Object[] { 103 });
                        try {
                            assertTrue(dataSet.next());
                            Row row = dataSet.getRow();
                            assertNotNull(row);
                            assertEquals("Atelier graphique", row.getValue(0).toString());
                            assertFalse(dataSet.next());
                        } finally {
                            dataSet.close();
                        }
                    }
                } catch (Throwable e) {
                    errors.add(e);
                } finally {
                    latch.countDown();
                }
            }
        };

        for (int i = 0; i < threadCount; i++) {
            executorService.execute(runnable);
        }

        try {
            latch.await(60000, TimeUnit.MILLISECONDS);

            if (errors.size() > 0) {
                throw new IllegalStateException(errors.get(0));
            }
            assertTrue(true);
        } finally {
            executorService.shutdownNow();
        }

        assertEquals(0, compiledQuery.getActiveLeases());

        compiledQuery.close();

        assertEquals(0, compiledQuery.getActiveLeases());
        assertEquals(0, compiledQuery.getIdleLeases());
    }

}