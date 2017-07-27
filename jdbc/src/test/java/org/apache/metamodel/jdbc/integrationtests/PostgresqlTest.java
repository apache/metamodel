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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.*;
import java.util.concurrent.TimeUnit;

import javax.swing.table.TableModel;

import org.apache.metamodel.BatchUpdateScript;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateSummary;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetTableModel;
import org.apache.metamodel.drop.DropTable;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.jdbc.JdbcTestTemplates;
import org.apache.metamodel.jdbc.QuerySplitter;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.OrderByItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.junit.Ignore;

/**
 * Test case that tests postgresql interaction. The test requires the
 * "dellstore2" sample database that can be found at pgfoundry.
 * 
 * @see http://mirrors.dotsrc.org/postgresql/projects/pgFoundry/dbsamples/
 */
public class PostgresqlTest extends AbstractJdbIntegrationTest {

    private static final String PROPERTY_LONGRUNNINGTESTS = "jdbc.postgresql.longrunningtests";
    private static final double DELTA = 1E-15;

    @Override
    protected String getPropertyPrefix() {
        return "postgresql";
    }

    public void testTimestampValueInsertSelect() throws Exception {
        if (!isConfigured()) {
            return;
        }

        final Connection connection = getConnection();
        JdbcTestTemplates.timestampValueInsertSelect(connection, TimeUnit.MICROSECONDS);
    }

    public void testCreateInsertAndUpdate() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcTestTemplates.simpleCreateInsertUpdateAndDrop(getDataContext(), "metamodel_test_simple");
    }

    public void testCompositePrimaryKeyCreation() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcTestTemplates.compositeKeyCreation(getDataContext(), "metamodel_test_composite_keys");
    }

    public void testInterpretationOfNull() throws Exception {
        if (!isConfigured()) {
            return;
        }
        JdbcTestTemplates.interpretationOfNulls(getConnection());
    }

    private JdbcDataContext createLimitAndOffsetTestData() {
        final JdbcDataContext dc = new JdbcDataContext(getConnection());

        if (dc.getTableByQualifiedLabel("test_table") != null) {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback callback) {
                    callback.dropTable("test_table").execute();
                }
            });
        }

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                Table table = callback.createTable(dc.getDefaultSchema(), "test_table").withColumn("foo").ofType(
                        ColumnType.INTEGER).withColumn("bar").ofType(ColumnType.VARCHAR).execute();
                callback.insertInto(table).value("foo", 1).value("bar", "hello").execute();
                callback.insertInto(table).value("foo", 2).value("bar", "there").execute();
                callback.insertInto(table).value("foo", 3).value("bar", "world").execute();
            }
        });

        dc.refreshSchemas();

        return dc;
    }

    public void testLimit() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcDataContext dc = createLimitAndOffsetTestData();
        Schema schema = dc.getDefaultSchema();
        Table productsTable = schema.getTableByName("test_table");

        DataSet ds = dc.query().from(productsTable).select("foo").limit(2).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[2]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();
    }

    public void testOffset() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcDataContext dc = createLimitAndOffsetTestData();
        Schema schema = dc.getDefaultSchema();
        Table productsTable = schema.getTableByName("test_table");

        DataSet ds = dc.query().from(productsTable).select("foo").offset(1).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[2]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[3]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();
    }

    public void testLimitAndOffset() throws Exception {
        if (!isConfigured()) {
            return;
        }
        JdbcDataContext dc = createLimitAndOffsetTestData();
        Schema schema = dc.getDefaultSchema();
        Table productsTable = schema.getTableByName("test_table");

        DataSet ds = dc.query().from(productsTable).select("foo").limit(1).offset(1).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[2]]", ds.getRow().toString());
        assertFalse(ds.next());

        ds.close();
    }

    public void testQuotedInsertSyntax() throws Exception {
        if (!isConfigured()) {
            return;
        }
        final Connection connection = getConnection();

        try {
            connection.createStatement().execute("DROP TABLE my_table");
        } catch (Exception e) {
            // do nothing
        }

        JdbcDataContext dc = new JdbcDataContext(connection);
        final Schema schema = dc.getDefaultSchema();

        // create table
        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                Table table = cb.createTable(schema, "my_table").withColumn("id").asPrimaryKey().ofType(
                        ColumnType.INTEGER).ofNativeType("SERIAL").nullable(false).withColumn("name").ofType(
                                ColumnType.VARCHAR).ofSize(10).withColumn("foo").ofType(ColumnType.BOOLEAN).nullable(
                                        true).withColumn("bar").ofType(ColumnType.BOOLEAN).nullable(true).execute();

                assertEquals("my_table", table.getName());
            }
        });

        assertTrue(dc.getColumnByQualifiedLabel("my_table.id").isPrimaryKey());
        assertFalse(dc.getColumnByQualifiedLabel("my_table.name").isPrimaryKey());

        // insert records
        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                RowInsertionBuilder builder = callback.insertInto("my_table").value("name", "row 1").value("foo", true);

                try {
                    Method method = builder.getClass().getDeclaredMethod("createSqlStatement");
                    method.setAccessible(true);
                    Object result = method.invoke(builder);
                    assertEquals("INSERT INTO \"public\".\"my_table\" (name,foo) VALUES (?,?)", result.toString());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                builder.execute();

                callback.insertInto("my_table").value("name", "row 2").value("foo", false).execute();
            }
        });

        // query
        DataSet ds = dc.query().from("my_table").select("name").where("foo").eq(true).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[row 1]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        // drop
        dc.executeUpdate(new UpdateScript() {

            @Override
            public void run(UpdateCallback callback) {
                callback.dropTable("my_table").execute();
            }
        });
    }

    public void testInsertOfDifferentTypes() throws Exception {
        if (!isConfigured()) {
            return;
        }

        final Connection connection = getConnection();

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
                        .ofNativeType("SERIAL").nullable(false).withColumn("name").ofType(ColumnType.VARCHAR).ofSize(10)
                        .withColumn("foo").ofType(ColumnType.BOOLEAN).nullable(true).withColumn("bar").ofType(
                                ColumnType.BOOLEAN).nullable(true).execute();

                assertEquals("my_table", table.getName());
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
            assertEquals("Row[values=[1, row 1]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[2, row 2]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[3, row 3]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[4, row 4]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[5, row 5]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[6, row 6]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[7, row 7]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[8, row 8]]", ds.getRow().toString());
            assertFalse(ds.next());
            ds.close();
        } finally {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback callback) {
                    callback.dropTable("my_table").execute();
                }
            });
        }
    }

    public void testJsonAndJsonbDatatypes() throws Exception {
        if (!isConfigured()) {
            return;
        }

        final JdbcDataContext dc = new JdbcDataContext(getConnection());

        final Schema schema = dc.getDefaultSchema();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                final Table table = cb.createTable(schema, "json_datatypes_table").withColumn("id").ofType(ColumnType.INTEGER)
                        .ofNativeType("SERIAL").asPrimaryKey().nullable(false).withColumn("col_json").ofNativeType(
                                "json").withColumn("col_jsonb").ofNativeType("jsonb").execute();
                assertEquals("json_datatypes_table", table.getName());

                final Map<String, Object> map = new HashMap<>();
                map.put("foo", "bar");
                cb.insertInto(table).value("id", 1).value("col_json", map).execute();
                cb.insertInto(table).value("id", 2).value("col_jsonb", "{'foo':'baz'}".replace('\'', '"')).execute();
            }
        });

        try {
            final DataSet ds = dc.query().from("json_datatypes_table").select("col_json", "col_jsonb").execute();
            
            assertTrue(ds.next());
            assertEquals("Row[values=[{foo=bar}, null]]", ds.getRow().toString());
            assertTrue(ds.getRow().getValue(0) instanceof Map);
            assertTrue(ds.next());
            assertEquals("Row[values=[null, {foo=baz}]]", ds.getRow().toString());
            assertTrue(ds.getRow().getValue(1) instanceof Map);
            assertFalse(ds.next());
        } finally {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback cb) {
                    cb.dropTable("json_datatypes_table").execute();
                }
            });
        }

    }

    /**
     * Tests some inconsistencies dealing with booleans.
     * 
     * @see http://eobjects.org/trac/ticket/829
     */
    public void testBoolean() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcDataContext dc = new JdbcDataContext(getConnection());

        final Schema schema = dc.getDefaultSchema();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                Table table = cb.createTable(schema, "my_table").withColumn("id").ofType(ColumnType.INTEGER)
                        .ofNativeType("SERIAL").nullable(false).withColumn("some_bool").ofType(ColumnType.BOOLEAN)
                        .nullable(false).execute();
                assertEquals("my_table", table.getName());

                cb.insertInto(table).value("id", 1).value("some_bool", true).execute();
                cb.insertInto(table).value("id", 2).value("some_bool", false).execute();
            }
        });

        DataSet ds = dc.query().from("my_table").select("some_bool").execute();

        assertTrue(ds.next());
        assertEquals("Row[values=[true]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[false]]", ds.getRow().toString());
        assertFalse(ds.next());

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                cb.dropTable("my_table").execute();
            }
        });
    }

    /**
     * Tests type rewriting for double type.
     * 
     * @see https://issues.apache.org/jira/browse/METAMODEL-151
     */
    public void testDouble() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcDataContext dc = new JdbcDataContext(getConnection());

        final Schema schema = dc.getDefaultSchema();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                Table table = cb.createTable(schema, "my_table").withColumn("id").ofType(ColumnType.INTEGER)
                        .ofNativeType("SERIAL").nullable(false).withColumn("some_double").ofType(ColumnType.DOUBLE)
                        .nullable(false).execute();
                assertEquals("my_table", table.getName());

                cb.insertInto(table).value("id", 1).value("some_double", Double.MIN_VALUE).execute();
                cb.insertInto(table).value("id", 2).value("some_double", Double.MAX_VALUE).execute();
                cb.insertInto(table).value("id", 3).value("some_double", Double.NEGATIVE_INFINITY).execute();
                cb.insertInto(table).value("id", 4).value("some_double", Double.POSITIVE_INFINITY).execute();
                cb.insertInto(table).value("id", 5).value("some_double", Double.NaN).execute();
            }
        });

        try {
            DataSet ds = dc.query().from("my_table").select("some_double").execute();
            assertTrue(ds.next());
            Double minVal = (Double) ds.getRow().getValue(ds.getSelectItems().get(0));
            assertTrue(ds.next());
            Double maxVal = (Double) ds.getRow().getValue(ds.getSelectItems().get(0));
            assertTrue(ds.next());
            Double negInf = (Double) ds.getRow().getValue(ds.getSelectItems().get(0));
            assertTrue(ds.next());
            Double posInf = (Double) ds.getRow().getValue(ds.getSelectItems().get(0));
            assertTrue(ds.next());
            Double nAn = (Double) ds.getRow().getValue(ds.getSelectItems().get(0));
            assertFalse(ds.next());

            assertEquals(Double.MIN_VALUE, minVal, DELTA);
            assertEquals(Double.MAX_VALUE, maxVal, DELTA);
            assertTrue(Double.isInfinite(negInf));
            assertTrue(Double.isInfinite(posInf));
            assertTrue(Double.isNaN(nAn));
        } finally {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback cb) {
                    cb.dropTable("my_table").execute();
                }
            });
        }
    }

    public void testGetGeneratedKeys() throws Exception {
        if (!isConfigured()) {
            return;
        }

        final JdbcDataContext dc = new JdbcDataContext(getConnection());
        final Schema schema = dc.getDefaultSchema();
        final String tableName = "my_table_with_generated_keys";
        
        if (schema.getTableByName(tableName) != null) {
            dc.executeUpdate(new DropTable(schema, tableName));
        }

        final UpdateSummary updateSummary = dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                Table table = cb.createTable(schema, tableName).withColumn("id").ofType(ColumnType.INTEGER)
                        .ofNativeType("SERIAL").nullable(false).asPrimaryKey().withColumn("foo").ofType(
                                ColumnType.STRING).execute();
                assertEquals(tableName, table.getName());

                cb.insertInto(table).value("foo", "hello").execute();
                cb.insertInto(table).value("foo", "world").execute();
            }
        });

        final Optional<Integer> insertedRows = updateSummary.getInsertedRows();
        assertTrue(insertedRows.isPresent());
        assertEquals(2, insertedRows.get().intValue());
        
        final Optional<Iterable<Object>> generatedKeys = updateSummary.getGeneratedKeys();
        assertTrue(generatedKeys.isPresent());
        assertEquals("[1, 2]", generatedKeys.get().toString());
        
    }

    public void testBlob() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcDataContext dc = new JdbcDataContext(getConnection());
        final Schema schema = dc.getDefaultSchema();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                Table table = cb.createTable(schema, "my_table").withColumn("id").ofType(ColumnType.INTEGER)
                        .ofNativeType("SERIAL").nullable(false).withColumn("some_bytes").ofType(ColumnType.BLOB)
                        .execute();
                assertEquals("my_table", table.getName());
            }
        });

        try {
            dc.refreshSchemas();
            final Column column = dc.getColumnByQualifiedLabel("my_table.some_bytes");
            assertEquals("Column[name=some_bytes,columnNumber=1,type=BINARY,nullable=true,"
                    + "nativeType=bytea,columnSize=2147483647]", column.toString());

            final Table table = column.getTable();

            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback callback) {
                    callback.insertInto(table).value(column, new byte[] { 1, 2, 3 }).execute();
                    callback.insertInto(table).value(column, "hello world".getBytes()).execute();
                }
            });

            byte[] bytes;

            DataSet ds = dc.query().from(table).select(table.getColumns()).execute();

            assertTrue(ds.next());
            assertEquals(1, ds.getRow().getValue(0));
            bytes = (byte[]) ds.getRow().getValue(1);
            assertEquals(3, bytes.length);
            assertEquals(1, bytes[0]);
            assertEquals(2, bytes[1]);
            assertEquals(3, bytes[2]);

            assertTrue(ds.next());
            assertEquals(2, ds.getRow().getValue(0));
            bytes = (byte[]) ds.getRow().getValue(1);

            assertEquals("hello world", new String(bytes));
            assertFalse(ds.next());

        } finally {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback cb) {
                    cb.dropTable("my_table").execute();
                }
            });
        }
    }

    public void testCreateInsertAndUpdateDateTypes() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcDataContext dataContext = getDataContext();
        JdbcTestTemplates.createInsertAndUpdateDateTypes(dataContext, dataContext.getDefaultSchema(),
                "metamodel_postgresql_test");
    }

    public void testCreateTableAndWriteRecords() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcDataContext dc = new JdbcDataContext(getConnection());
        final Schema schema = dc.getDefaultSchema();
        try {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback cb) {
                    Table table = cb.createTable(schema, "my_table").withColumn("id").ofType(ColumnType.INTEGER)
                            .ofNativeType("SERIAL").nullable(false).withColumn("person name").ofSize(255).withColumn(
                                    "age").ofType(ColumnType.INTEGER).execute();
                    assertEquals("[id, person name, age]", Arrays.toString(table.getColumnNames().toArray()));
                    assertEquals(
                            "Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=serial,columnSize=10]",
                            table.getColumnByName("id").toString());
                    assertEquals(
                            "Column[name=person name,columnNumber=1,type=VARCHAR,nullable=true,nativeType=varchar,columnSize=255]",
                            table.getColumnByName("person name").toString());
                    assertEquals(
                            "Column[name=age,columnNumber=2,type=INTEGER,nullable=true,nativeType=int4,columnSize=10]",
                            table.getColumnByName("age").toString());

                    cb.insertInto(table).value("person name", "John Doe").value("age", 42).execute();
                    cb.insertInto(table).value("age", 43).value("person name", "Jane Doe").execute();

                }
            });

            final Table table = schema.getTableByName("my_table");
            Query query = dc.query().from(table).select(table.getColumns()).toQuery();
            DataSet ds = dc.executeQuery(query);
            assertTrue(ds.next());
            assertEquals("Row[values=[1, John Doe, 42]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[2, Jane Doe, 43]]", ds.getRow().toString());
            assertFalse(ds.next());
            ds.close();

            dc.executeUpdate(new UpdateScript() {

                @Override
                public void run(UpdateCallback callback) {
                    callback.update(table).value("age", 102).where("id").eq(1).execute();
                    callback.deleteFrom(table).where("id").eq(2).execute();
                }
            });

            ds = dc.executeQuery(query);
            assertTrue(ds.next());
            assertEquals("Row[values=[1, John Doe, 102]]", ds.getRow().toString());
            assertFalse(ds.next());
            ds.close();
        } finally {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback callback) {
                    callback.dropTable("my_table").execute();
                }
            });
            assertNull(dc.getTableByQualifiedLabel("my_table"));
        }
    }

    public void testCreateTableInsertValueFloatForIntColumn() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcDataContext dc = new JdbcDataContext(getConnection());
        final Schema schema = dc.getDefaultSchema();
        try {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback cb) {
                    Table table = cb.createTable(schema, "my_table").withColumn("id").ofType(ColumnType.INTEGER)
                            .ofNativeType("SERIAL").nullable(false).withColumn("person name").ofSize(255).withColumn(
                                    "age").ofType(ColumnType.INTEGER).execute();
                    assertEquals("[id, person name, age]", Arrays.toString(table.getColumnNames().toArray()));
                    assertEquals(
                            "Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=serial,columnSize=10]",
                            table.getColumnByName("id").toString());
                    assertEquals(
                            "Column[name=person name,columnNumber=1,type=VARCHAR,nullable=true,nativeType=varchar,columnSize=255]",
                            table.getColumnByName("person name").toString());
                    assertEquals(
                            "Column[name=age,columnNumber=2,type=INTEGER,nullable=true,nativeType=int4,columnSize=10]",
                            table.getColumnByName("age").toString());

                    cb.insertInto(table).value("person name", "John Doe").value("age", 42.4673).execute();
                    cb.insertInto(table).value("age", 43.5673).value("person name", "Jane Doe").execute();
                }
            });

            Table table = schema.getTableByName("my_table");
            Query query = dc.query().from(table).select(table.getColumns()).toQuery();
            DataSet ds = dc.executeQuery(query);
            assertTrue(ds.next());
            // Float value input will be rounded down into integer number.
            assertEquals("Row[values=[1, John Doe, 42]]", ds.getRow().toString());
            assertTrue(ds.next());
            // The age will be incremented as float value input will be rounded
            // up.
            assertEquals("Row[values=[2, Jane Doe, 44]]", ds.getRow().toString());
            assertFalse(ds.next());

            ds.close();
        } finally {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback cb) {
                    cb.dropTable("my_table").execute();
                }
            });
        }
    }

    public void testInsertFailureForStringValueForIntegerColumn() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcDataContext dc = new JdbcDataContext(getConnection());
        final Schema schema = dc.getDefaultSchema();
        try {
            dc.executeUpdate(new BatchUpdateScript() {
                @Override
                public void run(UpdateCallback cb) {
                    Table table = cb.createTable(schema, "my_table").withColumn("id").ofType(ColumnType.INTEGER)
                            .ofNativeType("SERIAL").nullable(false).withColumn("person name").ofSize(255).withColumn(
                                    "age").ofType(ColumnType.INTEGER).execute();
                    assertEquals("[id, person name, age]", Arrays.toString(table.getColumnNames().toArray()));
                    assertEquals(
                            "Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=serial,columnSize=10]",
                            table.getColumnByName("id").toString());
                    assertEquals(
                            "Column[name=person name,columnNumber=1,type=VARCHAR,nullable=true,nativeType=varchar,columnSize=255]",
                            table.getColumnByName("person name").toString());
                    assertEquals(
                            "Column[name=age,columnNumber=2,type=INTEGER,nullable=true,nativeType=int4,columnSize=10]",
                            table.getColumnByName("age").toString());

                    cb.insertInto(table).value("person name", "John Doe").value("age", "42").execute();
                }
            });

        } catch (Exception e) {
            String message = e.getMessage().replaceAll("\n", " ");
            assertEquals(
                    "Could not execute batch: INSERT INTO \"public\".\"my_table\" (\"person name\",age) VALUES ('John Doe','42'): Batch entry 0 INSERT INTO \"public\".\"my_table\" (\"person name\",age) VALUES ('John Doe','42') was aborted.  Call getNextException to see the cause.",
                    message);
        } finally {
            dc.refreshSchemas();
            if (dc.getTableByQualifiedLabel("my_table") != null) {
                dc.executeUpdate(new UpdateScript() {
                    @Override
                    public void run(UpdateCallback cb) {
                        cb.dropTable("my_table").execute();
                    }
                });
            }
        }
    }

    public void testDatabaseProductName() throws Exception {
        if (!isConfigured()) {
            return;
        }

        String databaseProductName = getConnection().getMetaData().getDatabaseProductName();
        assertEquals(JdbcDataContext.DATABASE_PRODUCT_POSTGRESQL, databaseProductName);
    }

    public void testGetDefaultSchema() throws Exception {
        if (!isConfigured()) {
            return;
        }

        DataContext dc = new JdbcDataContext(getConnection());
        Schema schema = dc.getDefaultSchema();
        assertEquals("public", schema.getName());
    }

    public void testGetSchema() throws Exception {
        if (!isConfigured()) {
            return;
        }

        DataContext dc = new JdbcDataContext(getConnection());
        List<Schema> schemas = dc.getSchemas();
        assertTrue(schemas.size() >= 3);

        assertNotNull(dc.getSchemaByName("information_schema"));
        assertNotNull(dc.getSchemaByName("pg_catalog"));
        assertNotNull(dc.getSchemaByName("public"));

        Schema schema = dc.getSchemaByName("public");

        assertEquals("[Table[name=categories,type=TABLE,remarks=null], "
                + "Table[name=cust_hist,type=TABLE,remarks=null], " + "Table[name=customers,type=TABLE,remarks=null], "
                + "Table[name=inventory,type=TABLE,remarks=null], " + "Table[name=orderlines,type=TABLE,remarks=null], "
                + "Table[name=orders,type=TABLE,remarks=null], " + "Table[name=products,type=TABLE,remarks=null], "
                + "Table[name=reorder,type=TABLE,remarks=null]]", Arrays.toString(schema.getTables().toArray()));

        Table productsTable = schema.getTableByName("products");
        assertEquals(
                "[Column[name=prod_id,columnNumber=0,type=INTEGER,nullable=false,nativeType=serial,columnSize=10], "
                        + "Column[name=category,columnNumber=1,type=INTEGER,nullable=false,nativeType=int4,columnSize=10], "
                        + "Column[name=title,columnNumber=2,type=VARCHAR,nullable=false,nativeType=varchar,columnSize=50], "
                        + "Column[name=actor,columnNumber=3,type=VARCHAR,nullable=false,nativeType=varchar,columnSize=50], "
                        + "Column[name=price,columnNumber=4,type=NUMERIC,nullable=false,nativeType=numeric,columnSize=12], "
                        + "Column[name=special,columnNumber=5,type=SMALLINT,nullable=true,nativeType=int2,columnSize=5], "
                        + "Column[name=common_prod_id,columnNumber=6,type=INTEGER,nullable=false,nativeType=int4,columnSize=10]]",
                Arrays.toString(productsTable.getColumns().toArray()));
        Table customersTable = schema.getTableByName("customers");
        assertEquals(
                "[Column[name=customerid,columnNumber=0,type=INTEGER,nullable=false,nativeType=serial,columnSize=10], "
                        + "Column[name=firstname,columnNumber=1,type=VARCHAR,nullable=false,nativeType=varchar,columnSize=50], "
                        + "Column[name=lastname,columnNumber=2,type=VARCHAR,nullable=false,nativeType=varchar,columnSize=50], "
                        + "Column[name=address1,columnNumber=3,type=VARCHAR,nullable=false,nativeType=varchar,columnSize=50], "
                        + "Column[name=address2,columnNumber=4,type=VARCHAR,nullable=true,nativeType=varchar,columnSize=50], "
                        + "Column[name=city,columnNumber=5,type=VARCHAR,nullable=false,nativeType=varchar,columnSize=50], "
                        + "Column[name=state,columnNumber=6,type=VARCHAR,nullable=true,nativeType=varchar,columnSize=50], "
                        + "Column[name=zip,columnNumber=7,type=INTEGER,nullable=true,nativeType=int4,columnSize=10], "
                        + "Column[name=country,columnNumber=8,type=VARCHAR,nullable=false,nativeType=varchar,columnSize=50], "
                        + "Column[name=region,columnNumber=9,type=SMALLINT,nullable=false,nativeType=int2,columnSize=5], "
                        + "Column[name=email,columnNumber=10,type=VARCHAR,nullable=true,nativeType=varchar,columnSize=50], "
                        + "Column[name=phone,columnNumber=11,type=VARCHAR,nullable=true,nativeType=varchar,columnSize=50], "
                        + "Column[name=creditcardtype,columnNumber=12,type=INTEGER,nullable=false,nativeType=int4,columnSize=10], "
                        + "Column[name=creditcard,columnNumber=13,type=VARCHAR,nullable=false,nativeType=varchar,columnSize=50], "
                        + "Column[name=creditcardexpiration,columnNumber=14,type=VARCHAR,nullable=false,nativeType=varchar,columnSize=50], "
                        + "Column[name=username,columnNumber=15,type=VARCHAR,nullable=false,nativeType=varchar,columnSize=50], "
                        + "Column[name=password,columnNumber=16,type=VARCHAR,nullable=false,nativeType=varchar,columnSize=50], "
                        + "Column[name=age,columnNumber=17,type=SMALLINT,nullable=true,nativeType=int2,columnSize=5], "
                        + "Column[name=income,columnNumber=18,type=INTEGER,nullable=true,nativeType=int4,columnSize=10], "
                        + "Column[name=gender,columnNumber=19,type=VARCHAR,nullable=true,nativeType=varchar,columnSize=1]]",
                Arrays.toString(customersTable.getColumns().toArray()));
        List<Relationship> relations = new ArrayList<>(customersTable.getRelationships());
        // bit o a hack to ensure ordering
        Collections.sort(relations, (rel1,rel2) -> rel1.getForeignTable().getName().compareTo(rel2.getForeignTable().getName()));
        assertEquals(2, relations.size());
        assertEquals(
                "[Relationship[primaryTable=customers,primaryColumns=[customerid],foreignTable=cust_hist,foreignColumns=[customerid]], "
                        + "Relationship[primaryTable=customers,primaryColumns=[customerid],foreignTable=orders,foreignColumns=[customerid]]]",
                Arrays.toString(relations.toArray()));
        assertEquals("Table[name=customers,type=TABLE,remarks=null]", relations.get(0).getPrimaryTable().toString());
        assertEquals("Table[name=cust_hist,type=TABLE,remarks=null]", relations.get(0).getForeignTable().toString());
        assertEquals("Table[name=customers,type=TABLE,remarks=null]", relations.get(1).getPrimaryTable().toString());
        assertEquals("Table[name=orders,type=TABLE,remarks=null]", relations.get(1).getForeignTable().toString());

        Table ordersTable = schema.getTableByName("orderlines");
        assertEquals(
                "[Column[name=orderlineid,columnNumber=0,type=INTEGER,nullable=false,nativeType=int4,columnSize=10], "
                        + "Column[name=orderid,columnNumber=1,type=INTEGER,nullable=false,nativeType=int4,columnSize=10], "
                        + "Column[name=prod_id,columnNumber=2,type=INTEGER,nullable=false,nativeType=int4,columnSize=10], "
                        + "Column[name=quantity,columnNumber=3,type=SMALLINT,nullable=false,nativeType=int2,columnSize=5], "
                        + "Column[name=orderdate,columnNumber=4,type=DATE,nullable=false,nativeType=date,columnSize=13]]",
                Arrays.toString(ordersTable.getColumns().toArray()));
    }

    public void testExecuteQueryInPublicSchema() throws Exception {
        if (!isConfigured()) {
            return;
        }

        DataContext dc = new JdbcDataContext(getConnection());
        Query q = new Query();
        Schema schema = dc.getSchemaByName("public");
        Table productsTable = schema.getTableByName("products");
        q.from(productsTable);

        Column titleColumn = productsTable.getColumnByName("title");
        Column productPriceColumn = productsTable.getColumnByName("price");
        q.select(titleColumn, productPriceColumn);
        q.getSelectClause().getItem(0).setAlias("product-title");

        DataSet data = dc.executeQuery(q);
        TableModel tableModel = new DataSetTableModel(data);
        assertEquals(2, tableModel.getColumnCount());
        assertEquals(10000, tableModel.getRowCount());

        assertEquals("ACADEMY ACADEMY", tableModel.getValueAt(0, 0).toString());
        assertEquals("25.99", tableModel.getValueAt(0, 1).toString());

        assertEquals("ACADEMY HORN", tableModel.getValueAt(432, 0).toString());
        assertEquals("16.99", tableModel.getValueAt(6346, 1).toString());

        assertEquals("ALADDIN ZORRO", tableModel.getValueAt(9999, 0).toString());
        assertEquals("10.99", tableModel.getValueAt(9999, 1).toString());

        data = null;
        tableModel = null;

        Column prodIdColumn = productsTable.getColumnByName("prod_id");
        Table orderlinesTable = schema.getTableByName("orderlines");
        Column commonProdIdColumn = orderlinesTable.getColumnByName("prod_id");
        Column quantityColumn = orderlinesTable.getColumnByName("quantity");

        q.from(orderlinesTable);
        q.where(new FilterItem(new SelectItem(prodIdColumn), OperatorType.EQUALS_TO, new SelectItem(
                commonProdIdColumn)));
        q.groupBy(titleColumn);
        q.getSelectClause().removeItem(q.getSelectClause().getSelectItem(productPriceColumn));
        SelectItem quantitySum = new SelectItem(FunctionType.SUM, quantityColumn).setAlias("orderAmount");
        q.select(quantitySum);
        q.having(new FilterItem(quantitySum, OperatorType.GREATER_THAN, 25));
        q.orderBy(new OrderByItem(q.getSelectClause().getItem(0)));

        assertEquals("SELECT \"products\".\"title\" AS product-title, SUM(\"orderlines\".\"quantity\") AS orderAmount "
                + "FROM public.\"products\", public.\"orderlines\" "
                + "WHERE \"products\".\"prod_id\" = \"orderlines\".\"prod_id\" " + "GROUP BY \"products\".\"title\" "
                + "HAVING SUM(\"orderlines\".\"quantity\") > 25 " + "ORDER BY \"products\".\"title\" ASC", q
                        .toString());
        data = dc.executeQuery(q);
        tableModel = new DataSetTableModel(data);
        assertEquals(2, tableModel.getColumnCount());
        assertEquals(136, tableModel.getRowCount());

        assertEquals("ACADEMY ALABAMA", tableModel.getValueAt(0, 0).toString());
        assertEquals("27", tableModel.getValueAt(0, 1).toString());

        assertEquals("AIRPORT MOURNING", tableModel.getValueAt(99, 0).toString());
        assertEquals("29", tableModel.getValueAt(99, 1).toString());

        assertEquals("ALADDIN WORKER", tableModel.getValueAt(135, 0).toString());
        assertEquals("27", tableModel.getValueAt(135, 1).toString());
    }

    public void testWhiteSpaceColumns() throws Exception {
        if (!isConfigured()) {
            return;
        }

        DatabaseMetaData metaData = getConnection().getMetaData();
        assertEquals("\"", metaData.getIdentifierQuoteString());
    }

    public void testCreateTableAndInsert1MRecords() throws Exception {
        if (!isConfigured()) {
            return;
        }

        if (!"true".equalsIgnoreCase(getProperties().getProperty(PROPERTY_LONGRUNNINGTESTS))) {
            return;
        }

        JdbcDataContext dc = new JdbcDataContext(getConnection());
        final Schema schema = dc.getDefaultSchema();
        try {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback cb) {
                    Table table = cb.createTable(schema, "my_table").withColumn("id").ofType(ColumnType.INTEGER)
                            .ofNativeType("SERIAL").nullable(false).withColumn("person name").ofSize(255).withColumn(
                                    "age").ofType(ColumnType.INTEGER).execute();
                    assertEquals("[id, person name, age]", Arrays.toString(table.getColumnNames().toArray()));
                    assertEquals(
                            "Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=serial,columnSize=10]",
                            table.getColumnByName("id").toString());
                    assertEquals(
                            "Column[name=person name,columnNumber=1,type=VARCHAR,nullable=true,nativeType=varchar,columnSize=255]",
                            table.getColumnByName("person name").toString());
                    assertEquals(
                            "Column[name=age,columnNumber=2,type=INTEGER,nullable=true,nativeType=int4,columnSize=10]",
                            table.getColumnByName("age").toString());

                    for (int i = 0; i < 1000000; i++) {
                        cb.insertInto(table).value("person name", "John Doe").value("age", i + 10).execute();
                    }

                }
            });

            Table table = schema.getTableByName("my_table");
            Query query = dc.query().from(table).selectCount().toQuery();
            DataSet ds = dc.executeQuery(query);
            assertTrue(ds.next());
            assertEquals("Row[values=[1000000]]", ds.getRow().toString());
            assertFalse(ds.next());
            ds.close();
        } finally {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback cb) {
                    cb.dropTable("my_table").execute();
                }
            });
        }
    }

    public void testCharOfSizeOne() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcTestTemplates.meaningOfOneSizeChar(getConnection());
    }

    /**
     * Splits a huge query into 146 pieces and executes them to test that the
     * collective result are equal to the original one in size
     */
    @Ignore
    public void testSplitHugeQueryExecute146() throws Exception {
        if (!isConfigured()) {
            return;
        }

        if (!"true".equalsIgnoreCase(getProperties().getProperty(PROPERTY_LONGRUNNINGTESTS))) {
            return;
        }

        DataContext dc = new JdbcDataContext(getConnection());
        Query q = new Query();
        Schema schema = dc.getSchemaByName("public");
        Table productsTable = schema.getTableByName("products");
        Table customerTable = schema.getTableByName("customers");
        q.from(productsTable, "p").from(customerTable, "c");

        Column titleColumn = productsTable.getColumnByName("title");
        Column priceColumn = productsTable.getColumnByName("price");
        Column cityColumn = customerTable.getColumnByName("city");
        Column ageColumn = customerTable.getColumnByName("age");
        q.select(titleColumn, priceColumn, cityColumn);

        q.where(new FilterItem(new SelectItem(priceColumn), OperatorType.GREATER_THAN, 27));
        q.where(new FilterItem(new SelectItem(ageColumn), OperatorType.GREATER_THAN, 55));

        assertEquals(
                "SELECT p.\"title\", p.\"price\", c.\"city\" FROM public.\"products\" p, public.\"customers\" c WHERE p.\"price\" > 27 AND c.\"age\" > 55",
                q.toString());

        QuerySplitter qs = new QuerySplitter(dc, q);
        qs.setMaxRows(100000);
        assertEquals(14072278, qs.getRowCount());

        List<Query> splitQueries = qs.splitQuery();
        assertEquals(146, splitQueries.size());
        assertEquals(
                "SELECT p.\"title\", p.\"price\", c.\"city\" FROM public.\"products\" p, public.\"customers\" c WHERE p.\"price\" > 27 AND c.\"age\" > 55 AND (c.\"customerid\" < 143 OR c.\"customerid\" IS NULL) AND (p.\"category\" < 8 OR p.\"category\" IS NULL)",
                splitQueries.get(0).toString());
        assertEquals(
                "SELECT p.\"title\", p.\"price\", c.\"city\" FROM public.\"products\" p, public.\"customers\" c WHERE p.\"price\" > 27 AND c.\"age\" > 55 AND (c.\"customerid\" > 19739 OR c.\"customerid\" = 19739)",
                splitQueries.get(145).toString());

        assertEquals(
                "[45954, 55752, 52122, 55480, 49770, 53410, 60434, 51590, 97284, 94336, 86966, 76648, 98758, 84018, 98758, 95810, 92862, 91388, 39798, 79596, "
                        + "91388, 48642, 60434, 106128, 94336, 94336, 86966, 79596, 85492, 94336, 104654, 97284, 84018, 101706, 109076, 89914, 110550, 107602, 98758, "
                        + "112024, 100232, 101706, 95810, 92862, 107602, 100232, 86966, 98758, 106128, 91388, 107602, 104654, 107602, 81070, 114972, 79596, 100232, 97284, "
                        + "103180, 98758, 113498, 103180, 89914, 104654, 97284, 109076, 114972, 103180, 86966, 106128, 101706, 95810, 103180, 88440, 112024, 91388, 106128, "
                        + "82544, 122342, 98758, 104654, 103180, 104654, 89914, 106128, 88440, 103180, 100232, 98758, 100232, 89914, 101706, 100232, 107602, 88440, 89914, "
                        + "91388, 103180, 100232, 104654, 120868, 106128, 100232, 107602, 97284, 103180, 106128, 91388, 100232, 106128, 100232, 109076, 94336, 106128, 94336, "
                        + "106128, 104654, 116446, 98758, 113498, 107602, 104654, 107602, 88440, 100232, 92862, 89914, 110550, 109076, 100232, 92862, 100232, 104654, 103180, "
                        + "89914, 103180, 103180, 107602, 85492, 112024, 85492, 101706, 92862, 86966, 104654, 201938]",
                Arrays.toString(getCounts(dc, splitQueries)));
        assertSameCount(dc, qs, splitQueries);

        DataSet data = qs.executeQueries(splitQueries);
        int count = 0;
        while (data.next()) {
            count++;
        }
        data.close();
        assertEquals(14072278, count);
        System.out.println("Successfully iterated 14072278 rows! :)");
    }

    /**
     * Utility method for asserting that a query and it's splitted queries have
     * the same total count
     */
    private void assertSameCount(DataContext dc, QuerySplitter qs, List<Query> queries) {
        long count1 = qs.getRowCount();
        long count2 = 0;
        for (Query q : queries) {
            count2 += getCount(dc, q);
        }
        assertEquals(count1, count2);
    }

    public long[] getCounts(DataContext dc, List<Query> queries) {
        long[] result = new long[queries.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = getCount(dc, queries.get(i));
        }
        return result;
    }

    /**
     * Gets the count of a query
     */
    private long getCount(DataContext dc, Query query) {
        return new QuerySplitter(dc, query).getRowCount();
    }
}