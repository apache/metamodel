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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.metamodel.BatchUpdateScript;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.create.ColumnCreationBuilder;
import org.apache.metamodel.create.CreateTable;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.drop.DropTable;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.Update;
import org.apache.metamodel.util.DateUtils;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.Month;
import org.junit.Ignore;

/**
 * Some reusable test methods
 */
@Ignore
public class JdbcTestTemplates {

    public static void interpretationOfNulls(Connection conn) throws Exception {
        final JdbcDataContext dc = new JdbcDataContext(conn);
        final Schema schema = dc.getDefaultSchema();

        if (dc.getTableByQualifiedLabel("test_table") != null) {
            dc.executeUpdate(new DropTable(schema, "test_table"));
        }

        final Map<Object, Object> map = new HashMap<Object, Object>();
        try {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback cb) {
                    ColumnCreationBuilder createTableBuilder = cb.createTable(schema, "test_table").withColumn("id")
                            .ofType(ColumnType.FLOAT).withColumn("code").ofType(ColumnType.VARCHAR).ofSize(10);
                    Table table = createTableBuilder.execute();

                    cb.insertInto(table).value("id", 1.0).value("code", "C01").execute();
                    cb.insertInto(table).value("id", 2.0).value("code", "C02").execute();
                    cb.insertInto(table).value("id", 3.0).value("code", null).execute();
                    cb.insertInto(table).value("id", 4.0).value("code", "C02").execute();
                }
            });

            assertEquals(1, getCount(dc.query().from("test_table").selectCount().where("code").isNull().execute()));
            assertEquals(3, getCount(dc.query().from("test_table").selectCount().where("code").isNotNull().execute()));
            assertEquals(2, getCount(dc.query().from("test_table").selectCount().where("code").ne("C02").execute()));

            // we put the results into a map, because databases are not in
            // agreement
            // wrt. if NULL is greater than or less than other values, so
            // ordering
            // does not help

            DataSet ds = dc.query().from("test_table").select("code").selectCount().groupBy("code").execute();
            assertTrue(ds.next());
            map.put(ds.getRow().getValue(0), ds.getRow().getValue(1));
            assertTrue(ds.next());
            map.put(ds.getRow().getValue(0), ds.getRow().getValue(1));
            assertTrue(ds.next());
            map.put(ds.getRow().getValue(0), ds.getRow().getValue(1));
            assertFalse(ds.next());

            ds.close();
        } finally {
            dc.executeUpdate(new DropTable(schema, "test_table"));
        }

        assertEquals(1, ((Number) map.get(null)).intValue());
        assertEquals(1, ((Number) map.get("C01")).intValue());
        assertEquals(2, ((Number) map.get("C02")).intValue());

        assertEquals(3, map.size());
    }

    private static int getCount(DataSet ds) {
        assertTrue(ds.next());
        Row row = ds.getRow();
        assertFalse(ds.next());
        ds.close();

        Number count = (Number) row.getValue(0);
        return count.intValue();
    }

    public static void differentOperatorsTest(Connection conn) throws Exception {
        assertNotNull(conn);

        assertFalse(conn.isReadOnly());

        final JdbcDataContext dc = new JdbcDataContext(conn);
        final Schema schema = dc.getDefaultSchema();

        if (dc.getTableByQualifiedLabel("test_table") != null) {
            dc.executeUpdate(new DropTable("test_table"));
        }

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                ColumnCreationBuilder createTableBuilder = cb.createTable(schema, "test_table").withColumn("id")
                        .ofType(ColumnType.FLOAT).withColumn("code").ofType(ColumnType.VARCHAR).ofSize(10);
                Table table = createTableBuilder.execute();

                cb.insertInto(table).value("id", 1.0).value("code", "C01").execute();
                cb.insertInto(table).value("id", 2.0).value("code", "C02").execute();
                cb.insertInto(table).value("id", 3.0).value("code", null).execute();
                cb.insertInto(table).value("id", 4.0).value("code", "C04").execute();
            }
        });

        DataSet ds;

        // regular EQUALS
        ds = dc.query().from(schema.getTableByName("test_table")).selectCount().where("code").eq("C02").execute();
        assertTrue(ds.next());
        assertEquals("1", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        // regular NOT EQUALS
        ds = dc.query().from(schema.getTableByName("test_table")).selectCount().where("code").ne("C02").execute();
        assertTrue(ds.next());
        assertEquals("3", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        // regular GREATER THAN
        ds = dc.query().from(schema.getTableByName("test_table")).selectCount().where("id").gt(2).execute();
        assertTrue(ds.next());
        assertEquals("2", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        // regular LESS THAN
        ds = dc.query().from(schema.getTableByName("test_table")).selectCount().where("id").lt(2).execute();
        assertTrue(ds.next());
        assertEquals("1", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        // IS NULL
        ds = dc.query().from(schema.getTableByName("test_table")).selectCount().where("code").isNull().execute();
        assertTrue(ds.next());
        assertEquals("1", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        // IS NOT NULL
        ds = dc.query().from(schema.getTableByName("test_table")).selectCount().where("code").isNotNull().execute();
        assertTrue(ds.next());
        assertEquals("3", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        // LIKE
        ds = dc.query().from(schema.getTableByName("test_table")).selectCount().where("code").like("C%").execute();
        assertTrue(ds.next());
        assertEquals("3", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        // NOT LIKE
        ds = dc.query().from(schema.getTableByName("test_table")).selectCount().where("code").notLike("%1").execute();
        assertTrue(ds.next());
        assertEquals("2", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        // regular IN (with string)
        ds = dc.query().from(schema.getTableByName("test_table")).selectCount().where("code").in("C01", "C02")
                .execute();
        assertTrue(ds.next());
        assertEquals("2", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        // regular IN (with decimals)
        ds = dc.query().from(schema.getTableByName("test_table")).selectCount().where("id").in(1.0, 2.0, 4.0).execute();
        assertTrue(ds.next());
        assertEquals("3", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        // regular NOT IN (with string)
        ds = dc.query().from(schema.getTableByName("test_table")).selectCount().where("code").notIn("C01", "C02")
                .execute();
        assertTrue(ds.next());
        assertEquals("1", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        // regular NOT IN (with decimals)
        ds = dc.query().from(schema.getTableByName("test_table")).selectCount().where("id").notIn(1.0, 2.0, 4.0).execute();
        assertTrue(ds.next());
        assertEquals("1", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        // irregular IN (with null value) - (currently uses SQL's standard way
        // of understanding NULL - see ticket #1058)
        Query query = dc.query().from(schema.getTableByName("test_table")).selectCount().where("code")
                .in("foobar", null, "baz").toQuery();
        String sql = dc.getQueryRewriter().rewriteQuery(query);

        assertTrue(sql, sql.endsWith(" IN ('foobar' , 'baz')"));
        ds = dc.executeQuery(query);
        assertTrue(ds.next());
        assertEquals("0", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();
    }

    public static void meaningOfOneSizeChar(Connection conn) throws Exception {
        assertNotNull(conn);

        assertFalse(conn.isReadOnly());

        final JdbcDataContext dc = new JdbcDataContext(conn);
        final Schema schema = dc.getDefaultSchema();

        if (dc.getTableByQualifiedLabel("test_table") != null) {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback cb) {
                    cb.dropTable("test_table").execute();
                }
            });
        }

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                ColumnCreationBuilder createTableBuilder = cb.createTable(schema, "test_table").withColumn("id")
                        .ofType(ColumnType.INTEGER).withColumn("code").ofType(ColumnType.CHAR).ofSize(1);
                String sql = createTableBuilder.toSql();
                assertTrue(sql, sql.indexOf("test_table (id INTEGER,code CHAR(1))") != -1);
                Table table = createTableBuilder.execute();

                cb.insertInto(table).value("id", 1).value("code", 'P').execute();
                cb.insertInto(table).value("id", 2).value("code", 'O').execute();
                cb.insertInto(table).value("id", 3).value("code", null).execute();
            }
        });

        DataSet ds = dc.query().from(schema.getTableByName("test_table")).select("code").orderBy("id").execute();
        assertTrue(ds.next());
        assertTrue(ds.getRow().getValue(0) instanceof String);
        assertTrue(ds.next());
        assertTrue(ds.getRow().getValue(0) instanceof String);
        assertTrue(ds.next());
        assertNull(ds.getRow().getValue(0));
        assertFalse(ds.next());
        ds.close();
    }

    public static void automaticConversionWhenInsertingString(Connection conn) throws Exception {
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
                        .withColumn("birthdate1").ofType(ColumnType.DATE).withColumn("birthdate2")
                        .ofType(ColumnType.TIMESTAMP).execute();

                cb.insertInto(table).value("id", "1").value("birthdate1", null).execute();
                cb.insertInto(table).value("id", 2).value("birthdate1", "2011-12-21")
                        .value("birthdate2", "2011-12-21 14:00:00").execute();
            }
        });

        DataSet ds = dc.query().from("test_table").select("id").and("birthdate1").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1, null]]", ds.getRow().toString());
        assertEquals("java.lang.Integer", ds.getRow().getValue(0).getClass().getName());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, 2011-12-21]]", ds.getRow().toString());
        assertEquals("java.sql.Date", ds.getRow().getValue(1).getClass().getName());
        assertFalse(ds.next());
        ds.close();

        Query query = dc.query().from("test_table").select("id").where("birthdate2")
                .lessThan(DateUtils.get(2011, Month.DECEMBER, 20)).toQuery();
        try {
            ds = dc.executeQuery(query);
        } catch (Exception e) {
            System.out.println("Failing query was: " + dc.getQueryRewriter().rewriteQuery(query));
            throw e;
        }
        assertFalse(ds.next());
        ds.close();

        ds = dc.query().from("test_table").select("id").where("birthdate2")
                .greaterThan(DateUtils.get(2011, Month.DECEMBER, 20)).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[2]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.deleteFrom("test_table").where("id").in(Arrays.<String> asList("1", "2")).execute();
            }
        });

        ds = dc.query().from("test_table").selectCount().where("id").eq(2).or("id").eq(1).execute();
        assertTrue(ds.next());
        assertEquals(0, ((Number) ds.getRow().getValue(0)).intValue());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.dropTable("test_table").execute();
            }
        });
    }

    public static void createInsertAndUpdateDateTypes(final JdbcDataContext dc, final Schema schema,
            final String tableName) throws Exception {
        if (schema.getTableByName(tableName) != null) {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback callback) {
                    callback.dropTable(schema.getTableByName(tableName)).execute();
                }
            });
        }

        dc.executeUpdate(new BatchUpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                Table table = cb.createTable(schema, tableName).withColumn("id").asPrimaryKey()
                        .ofType(ColumnType.INTEGER).withColumn("birthdate").ofType(ColumnType.DATE)
                        .withColumn("wakemeup").ofType(ColumnType.TIME).execute();

                // insert record 1
                {
                    // create a 7:55 time.
                    Calendar cal = Calendar.getInstance();
                    cal.setTimeInMillis(0);
                    cal.set(Calendar.HOUR_OF_DAY, 7);
                    cal.set(Calendar.MINUTE, 55);
                    Date wakeUpTime = cal.getTime();
                    cb.insertInto(table).value("id", 1).value("birthdate", DateUtils.get(1982, Month.APRIL, 20))
                            .value("wakemeup", wakeUpTime).execute();
                }

                // insert record 2
                {
                    // create a 18:35 time.
                    Calendar cal = Calendar.getInstance();
                    cal.setTimeInMillis(0);
                    cal.set(Calendar.HOUR_OF_DAY, 18);
                    cal.set(Calendar.MINUTE, 35);
                    Date wakeUpTime = cal.getTime();
                    cb.insertInto(table).value("id", 2).value("birthdate", DateUtils.get(1982, Month.APRIL, 21))
                            .value("wakemeup", wakeUpTime).execute();
                }
            }
        });

        try {

            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback cb) {
                    cb.insertInto(schema.getTableByName(tableName)).value("id", 3).value("birthdate", "2011-12-21")
                            .value("wakemeup", "12:00").execute();
                }
            });

            DataSet ds = dc.query().from(schema.getTableByName(tableName)).select("id", "birthdate", "wakemeup")
                    .orderBy("id").execute();
            assertTrue(ds.next());
            assertEquals("1", ds.getRow().getValue(0).toString());
            assertEquals("1982-04-20", ds.getRow().getValue(1).toString());
            assertTrue("Actual value was: " + ds.getRow().getValue(2),
                    ds.getRow().getValue(2).toString().startsWith("07:55:00"));

            assertTrue(ds.next());
            assertEquals("2", ds.getRow().getValue(0).toString());
            assertEquals("1982-04-21", ds.getRow().getValue(1).toString());
            assertTrue("Actual value was: " + ds.getRow().getValue(2),
                    ds.getRow().getValue(2).toString().startsWith("18:35:00"));

            assertTrue(ds.next());
            assertEquals("3", ds.getRow().getValue(0).toString());
            assertEquals("2011-12-21", ds.getRow().getValue(1).toString());
            assertTrue("Actual value was: " + ds.getRow().getValue(2),
                    ds.getRow().getValue(2).toString().startsWith("12:00"));

            assertFalse(ds.next());
            ds.close();

            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback callback) {
                    // update record 1

                    // create a 08:00 time.
                    Calendar cal = Calendar.getInstance();
                    cal.setTimeInMillis(0);
                    cal.set(Calendar.HOUR_OF_DAY, 8);
                    cal.set(Calendar.MINUTE, 00);
                    Date wakeUpTime = cal.getTime();

                    callback.update(schema.getTableByName(tableName))
                            .value("birthdate", DateUtils.get(1982, Month.APRIL, 21)).value("wakemeup", wakeUpTime)
                            .where("birthdate").isEquals(DateUtils.get(1982, Month.APRIL, 20)).execute();
                }
            });

            ds = dc.query().from(schema.getTableByName(tableName)).select("id", "birthdate", "wakemeup").orderBy("id")
                    .execute();
            assertTrue(ds.next());
            assertEquals("1", ds.getRow().getValue(0).toString());
            assertEquals("1982-04-21", ds.getRow().getValue(1).toString());
            assertTrue("Actual value was: " + ds.getRow().getValue(2),
                    ds.getRow().getValue(2).toString().startsWith("08:00:00"));

            assertTrue(ds.next());
            assertEquals("2", ds.getRow().getValue(0).toString());
            assertEquals("1982-04-21", ds.getRow().getValue(1).toString());
            assertTrue("Actual value was: " + ds.getRow().getValue(2),
                    ds.getRow().getValue(2).toString().startsWith("18:35:00"));

            assertTrue(ds.next());
            assertEquals("3", ds.getRow().getValue(0).toString());
            assertEquals("2011-12-21", ds.getRow().getValue(1).toString());
            assertTrue("Actual value was: " + ds.getRow().getValue(2),
                    ds.getRow().getValue(2).toString().startsWith("12:00"));

            assertFalse(ds.next());
            ds.close();

        } finally {
            if (schema.getTableByName(tableName) != null) {
                dc.executeUpdate(new UpdateScript() {
                    @Override
                    public void run(UpdateCallback callback) {
                        callback.dropTable(schema.getTableByName(tableName)).execute();
                    }
                });
            }
        }
    }

    public static void convertClobToString(JdbcDataContext dc) {
        System.setProperty(JdbcDataContext.SYSTEM_PROPERTY_CONVERT_LOBS, "true");

        final Schema schema = dc.getDefaultSchema();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                Table table = callback.createTable(schema, "clob_test_table").withColumn("id")
                        .ofType(ColumnType.INTEGER).asPrimaryKey().withColumn("foo").ofType(ColumnType.CLOB).execute();

                callback.insertInto(table).value("id", 1).value("foo", "baaaaz").execute();

                StringReader sr = new StringReader("foooooooabavlsdk\nflskmflsdk");
                callback.insertInto(table).value("id", 2).value("foo", sr).execute();
            }
        });

        DataSet ds;

        ds = dc.query().from(schema, "clob_test_table").selectCount().execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[2]]", ds.getRow().toString());
        ds.close();

        ds = dc.query().from(schema, "clob_test_table").select("id", "foo").orderBy("id").execute();
        assertTrue(ds.next());
        assertEquals(1, ds.getRow().getValue(0));
        final Object clobValue1 = ds.getRow().getValue(1);
        assertTrue(clobValue1 instanceof Clob || clobValue1 instanceof String);
        assertTrue(ds.next());
        assertEquals(2, ds.getRow().getValue(0));
        final Object clobValue2 = ds.getRow().getValue(1);
        assertTrue(clobValue2 instanceof Clob || clobValue2 instanceof String);
        assertFalse(ds.next());
        ds.close();

        ds = dc.query().from(schema, "clob_test_table").select("id", "foo").orderBy("id").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1, baaaaz]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, foooooooabavlsdk\nflskmflsdk]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.dropTable(schema, "clob_test_table").execute();
            }
        });

        System.setProperty(JdbcDataContext.SYSTEM_PROPERTY_CONVERT_LOBS, "");
    }

    public static void simpleCreateInsertUpdateAndDrop(final JdbcDataContext dataContext, final String testTableName) {
        final Schema defaultSchema = dataContext.getDefaultSchema();

        if (defaultSchema.getTableByName(testTableName) != null) {
            // clean up before
            dataContext.executeUpdate(new DropTable(defaultSchema, testTableName));
        }

        dataContext.executeUpdate(new CreateTable(defaultSchema, testTableName).withColumn("mykey")
                .ofType(ColumnType.INTEGER).nullable(false).asPrimaryKey().withColumn("name")
                .ofType(ColumnType.STRING).ofSize(20));
        try {
            final Table table = defaultSchema.getTableByName(testTableName);
            assertNotNull(table);

            // insert
            dataContext.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback callback) {
                    callback.insertInto(table).value("mykey", 1).value("name", "Apache").execute();
                    callback.insertInto(table).value("mykey", 2).value("name", "MetaModel").execute();
                }
            });

            // update
            dataContext.executeUpdate(new Update(table).value("name", "MM").where("mykey").eq(2));

            DataSet ds = dataContext.query().from(table).selectAll().orderBy("mykey").execute();
            assertTrue(ds.next());
            assertEquals("Row[values=[1, Apache]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[2, MM]]", ds.getRow().toString());
            assertFalse(ds.next());
            ds.close();

        } finally {
            // clean up after
            dataContext.executeUpdate(new DropTable(defaultSchema, testTableName));
        }
    }

    public static void compositeKeyCreation(JdbcDataContext dataContext, String testTableName) {
        final Schema defaultSchema = dataContext.getDefaultSchema();

        if (defaultSchema.getTableByName(testTableName) != null) {
            // clean up before
            dataContext.executeUpdate(new DropTable(defaultSchema, testTableName));
        }

        dataContext.executeUpdate(new CreateTable(defaultSchema, testTableName).withColumn("mykey1")
                .ofType(ColumnType.INTEGER).nullable(false).asPrimaryKey().withColumn("mykey2")
                .ofType(ColumnType.INTEGER).nullable(false).asPrimaryKey().withColumn("name")
                .ofType(ColumnType.VARCHAR).ofSize(20));
        try {
            final Table table = defaultSchema.getTableByName(testTableName);
            assertNotNull(table);

            Column[] primaryKeys = table.getPrimaryKeys();
            assertEquals(2, primaryKeys.length);
            assertEquals("mykey1", primaryKeys[0].getName().toLowerCase());
            assertEquals("mykey2", primaryKeys[1].getName().toLowerCase());

            // insert two records with unique values on both keys
            dataContext.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback callback) {
                    callback.insertInto(table).value("mykey1", 1).value("mykey2", 100).value("name", "Apache")
                            .execute();
                    callback.insertInto(table).value("mykey1", 2).value("mykey2", 101).value("name", "MetaModel")
                            .execute();
                }
            });

            // insert a record with non-unique value on key 2 only
            dataContext.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback callback) {
                    callback.insertInto(table).value("mykey1", 3).value("mykey2", 100).value("name", "Foo bar")
                            .execute();
                }
            });

            // update
            dataContext.executeUpdate(new Update(table).value("name", "MM").where("mykey1").eq(2));

            DataSet ds = dataContext.query().from(table).selectAll().orderBy("mykey1").execute();
            assertTrue(ds.next());
            assertEquals("Row[values=[1, 100, Apache]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[2, 101, MM]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[3, 100, Foo bar]]", ds.getRow().toString());
            assertFalse(ds.next());
            ds.close();

        } finally {
            // clean up after
            dataContext.executeUpdate(new DropTable(defaultSchema, testTableName));
        }
    }

    /**
     * 
     * @param conn
     * @param databasePrecision
     *            the precision with which the database can handle timestamp
     *            values. Expected values: {@link TimeUnit#SECONDS},
     *            {@link TimeUnit#MILLISECONDS}, {@link TimeUnit#MICROSECONDS}
     *            or {@link TimeUnit#NANOSECONDS}.
     * 
     * @throws Exception
     */
    public static void timestampValueInsertSelect(Connection conn, TimeUnit databasePrecision) throws Exception {
        timestampValueInsertSelect(conn, databasePrecision, null);
    }

    public static void timestampValueInsertSelect(Connection conn, TimeUnit databasePrecision, final String nativeType)
            throws Exception {
        assertNotNull(conn);

        final Statement statement = conn.createStatement();
        try {
            // clean up, if nescesary
            statement.execute("DROP TABLE test_table");
        } catch (SQLException e) {
            // do nothing
        } finally {
            FileHelper.safeClose(statement);
        }

        assertFalse(conn.isReadOnly());

        JdbcDataContext dc = new JdbcDataContext(conn);
        final Schema schema = dc.getDefaultSchema();

        final Timestamp timestamp1;
        switch (databasePrecision) {
        case SECONDS:
            timestamp1 = Timestamp.valueOf("2015-10-16 16:33:33");
            break;
        case MILLISECONDS:
            timestamp1 = Timestamp.valueOf("2015-10-16 16:33:33.456");
            break;
        case MICROSECONDS:
            timestamp1 = Timestamp.valueOf("2015-10-16 16:33:33.456001");
            break;
        case NANOSECONDS:
            timestamp1 = Timestamp.valueOf("2015-10-16 16:33:33.456001234");
            break;
        default:
            throw new UnsupportedOperationException("Unsupported database precision: " + databasePrecision);
        }

        final Timestamp timestamp2;
        switch (databasePrecision) {
        case SECONDS:
            timestamp2 = Timestamp.valueOf("2015-10-16 16:33:34");
            break;
        case MILLISECONDS:
            timestamp2 = Timestamp.valueOf("2015-10-16 16:33:34.683");
            break;
        case MICROSECONDS:
            timestamp2 = Timestamp.valueOf("2015-10-16 16:33:34.683005");
            break;
        case NANOSECONDS:
            timestamp2 = Timestamp.valueOf("2015-10-16 16:33:34.683005678");
            break;
        default:
            throw new UnsupportedOperationException("Unsupported database precision: " + databasePrecision);
        }

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                TableCreationBuilder tableBuilder = cb.createTable(schema, "test_table");
                tableBuilder.withColumn("id").ofType(ColumnType.INTEGER);
                tableBuilder.withColumn("insertiontime").ofType(ColumnType.TIMESTAMP);
                if (nativeType == null) {
                    tableBuilder.withColumn("insertiontime").ofType(ColumnType.TIMESTAMP);
                } else {
                    tableBuilder.withColumn("insertiontime").ofType(ColumnType.TIMESTAMP).ofNativeType(nativeType);
                }
                Table table = tableBuilder.execute();

                cb.insertInto(table).value("id", 1).value("insertiontime", timestamp1).execute();
                cb.insertInto(table).value("id", 2).value("insertiontime", timestamp2).execute();
            }
        });

        DataSet ds = dc.query().from("test_table").select("id").and("insertiontime").execute();
        assertTrue(ds.next());

        switch (databasePrecision) {
        case SECONDS:
            assertEquals("Row[values=[1, 2015-10-16 16:33:33]]", ds.getRow().toString());
            break;
        case MILLISECONDS:
            assertEquals("Row[values=[1, 2015-10-16 16:33:33.456]]", ds.getRow().toString());
            break;
        case MICROSECONDS:
            assertEquals("Row[values=[1, 2015-10-16 16:33:33.456001]]", ds.getRow().toString());
            break;
        case NANOSECONDS:
            assertEquals("Row[values=[1, 2015-10-16 16:33:33.456001234]]", ds.getRow().toString());
            break;
        default:
            throw new UnsupportedOperationException("Unsupported database precision: " + databasePrecision);
        }
        assertTrue(ds.getRow().getValue(0) instanceof Number);
        assertTrue(ds.next());

        switch (databasePrecision) {
        case SECONDS:
            assertEquals("Row[values=[2, 2015-10-16 16:33:34]]", ds.getRow().toString());
            break;
        case MILLISECONDS:
            assertEquals("Row[values=[2, 2015-10-16 16:33:34.683]]", ds.getRow().toString());
            break;
        case MICROSECONDS:
            assertEquals("Row[values=[2, 2015-10-16 16:33:34.683005]]", ds.getRow().toString());
            break;
        case NANOSECONDS:
            assertEquals("Row[values=[2, 2015-10-16 16:33:34.683005678]]", ds.getRow().toString());
            break;
        default:
            throw new UnsupportedOperationException("Unsupported database precision: " + databasePrecision);
        }
        assertFalse(ds.next());
        ds.close();

        if (databasePrecision != TimeUnit.SECONDS) {
            Query query = dc.query().from("test_table").select("id").where("insertiontime").lessThan(timestamp2)
                    .toQuery();
            try {
                ds = dc.executeQuery(query);
            } catch (Exception e) {
                System.out.println("Failing query was: " + dc.getQueryRewriter().rewriteQuery(query));
                throw e;
            }
            assertTrue(ds.next());
            assertEquals("Row[values=[1]]", ds.getRow().toString());
            assertFalse(ds.next());
            ds.close();

            ds = dc.query().from("test_table").select("id").where("insertiontime").greaterThan(timestamp1).execute();
            assertTrue(ds.next());
            assertEquals("Row[values=[2]]", ds.getRow().toString());
            assertFalse(ds.next());
            ds.close();

            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback callback) {
                    callback.deleteFrom("test_table").where("insertiontime").eq(timestamp1).execute();
                }
            });

            ds = dc.query().from("test_table").selectCount().execute();
            assertTrue(ds.next());
            assertEquals("Row[values=[1]]", ds.getRow().toString());
            assertFalse(ds.next());
            ds.close();
        }

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.dropTable("test_table").execute();
            }
        });
    }
}
