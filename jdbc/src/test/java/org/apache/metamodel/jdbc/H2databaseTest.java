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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.create.CreateTable;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.delete.DeleteFrom;
import org.apache.metamodel.drop.DropTable;
import org.apache.metamodel.insert.InsertInto;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.Update;
import org.apache.metamodel.util.MutableRef;

import junit.framework.TestCase;

/**
 * Test case that tests interaction with the H2 embedded database
 */
public class H2databaseTest extends TestCase {
    
    public static final String DRIVER_CLASS = "org.h2.Driver";
    public static final String URL_MEMORY_DATABASE = "jdbc:h2:mem:";

    private final String[] FIRST_NAMES = { "Suzy", "Barbara", "John", "Ken", "Billy", "Larry", "Joe", "Margareth", "Bobby",
            "Elizabeth" };
    private final String[] LAST_NAMES = { "Doe", "Gates", "Jobs", "Ellison", "Trump" };

    private Connection conn;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Class.forName(DRIVER_CLASS);
        conn = DriverManager.getConnection(URL_MEMORY_DATABASE);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        conn.close();
    }
    
    public void testCreateInsertAndUpdate() throws Exception {
        JdbcDataContext dc = new JdbcDataContext(conn);
        JdbcTestTemplates.simpleCreateInsertUpdateAndDrop(dc, "metamodel_test_simple");
    }

    public void testCompositePrimaryKeyCreation() throws Exception {
        JdbcDataContext dc = new JdbcDataContext(conn);
        JdbcTestTemplates.compositeKeyCreation(dc, "metamodel_test_composite_keys");
    }
    
    public void testTimestampValueInsertSelect() throws Exception {
        JdbcTestTemplates.timestampValueInsertSelect(conn, TimeUnit.NANOSECONDS);
    }

    public void testUsingSingleUpdates() throws Exception {
        final JdbcDataContext dc = new JdbcDataContext(conn);
        
        final Schema schema = dc.getDefaultSchema();
        dc.executeUpdate(new CreateTable(schema, "test_table").withColumn("id").ofType(ColumnType.VARCHAR));

        final Table table = schema.getTableByName("test_table");
        dc.executeUpdate(new InsertInto(table).value(0, "foo"));
        dc.executeUpdate(new InsertInto(table).value(0, "bar"));

        DataSet ds;

        ds = dc.query().from(table).selectCount().execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[2]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new DeleteFrom(table).where("id").eq("bar"));
        

        ds = dc.query().from(table).selectCount().execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new Update(table).where("id").eq("foo").value("id", "baz"));
        
        ds = dc.query().from(table).selectAll().execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[baz]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new DropTable(table));

        assertNull(schema.getTableByName("test_table"));
    }

    public void testScenario() throws Exception {
        int rowsAffected = conn.createStatement().executeUpdate(
                "CREATE TABLE test_table (id INTEGER AUTO_INCREMENT, name VARCHAR(255), age INTEGER)");
        assertEquals(0, rowsAffected);
        PreparedStatement p = conn.prepareStatement("INSERT INTO test_table (name, age) VALUES (?,?)");

        // insert 10,000 random names
        for (int i = 0; i < 10000; i++) {
            int randomAge = (int) (Math.random() * 100);
            String randomName = getRandomFirstName() + " " + getRandomLastName();
            insert(p, randomName, randomAge);
        }

        JdbcDataContext dc = new JdbcDataContext(conn);
        assertEquals("[INFORMATION_SCHEMA, PUBLIC]", Arrays.toString(dc.getSchemaNames()));

        Schema schema = dc.getDefaultSchema();
        assertEquals("PUBLIC", schema.getName());

        assertEquals("[TEST_TABLE]", Arrays.toString(schema.getTableNames()));

        Table table = schema.getTableByName("test_table");

        assertEquals("[ID, NAME, AGE]", Arrays.toString(table.getColumnNames()));

        Column idColumn = table.getColumnByName("ID");
        assertEquals("Column[name=ID,columnNumber=0,type=INTEGER,nullable=false,nativeType=INTEGER,columnSize=10]",
                idColumn.toString());
        Column nameColumn = table.getColumnByName("NAME");
        assertEquals("Column[name=NAME,columnNumber=1,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=255]",
                nameColumn.toString());
        Column ageColumn = table.getColumnByName("AGE");
        assertEquals("Column[name=AGE,columnNumber=2,type=INTEGER,nullable=true,nativeType=INTEGER,columnSize=10]",
                ageColumn.toString());

        Query q = dc.query().from(table).selectCount().and(FunctionType.MAX, ageColumn).and(FunctionType.MIN, ageColumn)
                .toQuery();
        assertEquals("SELECT COUNT(*), MAX(\"TEST_TABLE\".\"AGE\"), MIN(\"TEST_TABLE\".\"AGE\") FROM PUBLIC.\"TEST_TABLE\"",
                q.toSql());

        assertEquals(1, dc.getFetchSizeCalculator().getFetchSize(q));

        DataSet ds = dc.executeQuery(q);
        assertTrue(ds.next());
        Row row = ds.getRow();
        assertFalse(ds.next());

        assertEquals(10000, ((Number) row.getValue(0)).intValue());
        int maxAge = ((Number) row.getValue(1)).intValue();
        assertTrue("Maximum age was: " + maxAge, maxAge > 90 && maxAge <= 100);
        int minAge = ((Number) row.getValue(2)).intValue();
        assertTrue("Minimum age was: " + minAge, minAge < 10 && minAge >= 0);

        q = dc.query().from(table).as("t").select(ageColumn).selectCount().where(ageColumn).greaterThan(50).groupBy(ageColumn)
                .toQuery();
        assertEquals("SELECT t.\"AGE\", COUNT(*) FROM PUBLIC.\"TEST_TABLE\" t WHERE t.\"AGE\" > 50 GROUP BY t.\"AGE\"", q.toSql());

        ds = dc.executeQuery(q);
        List<Object[]> objectArrays = ds.toObjectArrays();
        assertTrue(objectArrays.size() <= 50);
        assertTrue(objectArrays.size() > 40);

        for (Object[] objects : objectArrays) {
            Integer age = (Integer) objects[0];
            assertTrue(age.intValue() > 50);
            Number count = (Number) objects[1];
            assertTrue(count.intValue() > 0);
        }
    }

    private String getRandomFirstName() {
        int randomIndex = (int) (Math.random() * FIRST_NAMES.length);
        return FIRST_NAMES[randomIndex];
    }

    private String getRandomLastName() {
        int randomIndex = (int) (Math.random() * LAST_NAMES.length);
        return LAST_NAMES[randomIndex];
    }

    private void insert(PreparedStatement p, String name, int age) throws SQLException {
        p.setString(1, name);
        p.setInt(2, age);
        p.executeUpdate();
    }

    public void testQueryRewriter() throws Exception {
        final JdbcDataContext dc = new JdbcDataContext(conn);

        final IQueryRewriter queryRewriter = dc.getQueryRewriter();
        assertEquals("H2QueryRewriter", queryRewriter.getClass().getSimpleName());

        assertTrue(queryRewriter.isFirstRowSupported());
        assertTrue(queryRewriter.isMaxRowsSupported());
    }

    public void testBothFirstRowAndMaxRows() throws Exception {
        final JdbcDataContext dc = new JdbcDataContext(conn);
        final Schema schema = dc.getDefaultSchema();
        dc.executeUpdate(new UpdateScript() {

            @Override
            public void run(UpdateCallback cb) {
                JdbcCreateTableBuilder createTableBuilder = (JdbcCreateTableBuilder) cb.createTable(schema, "test_table");
                Table writtenTable = createTableBuilder.withColumn("id").asPrimaryKey().ofType(ColumnType.INTEGER).execute();

                for (int i = 0; i < 10; i++) {
                    cb.insertInto(writtenTable).value("id", i + 1).execute();
                }
            }
        });

        Query q = dc.query().from("test_table").select("id").toQuery();
        q.setFirstRow(2);
        q.setMaxRows(4);
        DataSet ds = dc.executeQuery(q);
        assertTrue(ds.next());
        assertEquals("Row[values=[2]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[3]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[4]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[5]]", ds.getRow().toString());
        assertFalse(ds.next());

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                cb.dropTable("test_table").execute();
            }
        });
    }

    public void testCreateTable() throws Exception {
        assertFalse(conn.isReadOnly());

        JdbcDataContext dc = new JdbcDataContext(conn);
        final Schema schema = dc.getDefaultSchema();
        Table readTable = dc.getDefaultSchema().getTableByName("test_table");
        assertNull(readTable);

        final MutableRef<Table> writtenTableRef = new MutableRef<Table>();
        dc.executeUpdate(new UpdateScript() {

            @Override
            public void run(UpdateCallback cb) {
                JdbcCreateTableBuilder createTableBuilder = (JdbcCreateTableBuilder) cb.createTable(schema, "test_table");
                Table writtenTable = createTableBuilder.withColumn("id").asPrimaryKey().ofType(ColumnType.INTEGER)
                        .withColumn("name").ofSize(255).ofType(ColumnType.VARCHAR).withColumn("age").ofType(ColumnType.INTEGER)
                        .execute();
                String sql = createTableBuilder.createSqlStatement();
                assertEquals("CREATE TABLE PUBLIC.test_table (id INTEGER, name VARCHAR(255), age INTEGER, PRIMARY KEY(id))", sql);
                assertNotNull(writtenTable);
                assertEquals("[ID, NAME, AGE]", Arrays.toString(writtenTable.getColumnNames()));

                writtenTableRef.set(writtenTable);
            }
        });

        assertEquals("[TEST_TABLE]", Arrays.toString(dc.getDefaultSchema().getTableNames()));

        readTable = dc.getDefaultSchema().getTableByName("test_table");
        assertEquals("[ID, NAME, AGE]", Arrays.toString(readTable.getColumnNames()));
        assertEquals("[Column[name=ID,columnNumber=0,type=INTEGER,nullable=false,nativeType=INTEGER,columnSize=10]]",
                Arrays.toString(readTable.getPrimaryKeys()));
        assertEquals(writtenTableRef.get(), readTable);

        assertFalse(conn.isReadOnly());

        dc = new JdbcDataContext(conn);
        assertSame(conn, dc.getConnection());

        readTable = dc.getDefaultSchema().getTableByName("test_table");
        assertEquals("[ID, NAME, AGE]", Arrays.toString(readTable.getColumnNames()));
        assertTrue(writtenTableRef.get().getQualifiedLabel().equalsIgnoreCase(readTable.getQualifiedLabel()));

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                cb.insertInto(writtenTableRef.get()).value("age", 14).value("name", "hello").value("id", 1).execute();
                JdbcInsertBuilder insertBuilder = (JdbcInsertBuilder) cb.insertInto(writtenTableRef.get()).value("age", 15)
                        .value("name", "wor'ld").value("id", 2);
                assertEquals("INSERT INTO PUBLIC.\"TEST_TABLE\" (ID,NAME,AGE) VALUES (?,?,?)", insertBuilder.createSqlStatement());
                insertBuilder.execute();
                cb.insertInto(writtenTableRef.get()).value("age", 16).value("name", "escobar!").value("id", 3).execute();
            }
        });

        DataSet ds = dc.query().from(readTable).select(readTable.getColumns()).orderBy("id").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1, hello, 14]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, wor'ld, 15]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[3, escobar!, 16]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                JdbcUpdateBuilder updateCallback = (JdbcUpdateBuilder) callback.update("test_table").value("age", 18).where("id")
                        .greaterThan(1);
                assertEquals("UPDATE PUBLIC.\"TEST_TABLE\" SET AGE=? WHERE \"TEST_TABLE\".\"ID\" > ?",
                        updateCallback.createSqlStatement());
                updateCallback.execute();
            }
        });

        ds = dc.query().from(readTable).select(readTable.getColumns()).orderBy("id").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1, hello, 14]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, wor'ld, 18]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[3, escobar!, 18]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.deleteFrom("test_table").where("age").greaterThan(15).execute();
            }
        });

        ds = dc.query().from(readTable).select(readTable.getColumns()).orderBy("id").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1, hello, 14]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        assertEquals("[TEST_TABLE]", Arrays.toString(dc.getDefaultSchema().getTableNames()));

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.dropTable("test_table").execute();
            }
        });

        assertEquals("[]", Arrays.toString(dc.getDefaultSchema().getTableNames()));
    }

    public void testSelectItemsThatReferencesDifferentFromItems() throws Exception {
        assertNotNull(conn);

        try {
            // clean up, if nescesary
            conn.createStatement().execute("DROP TABLE test_table");
        } catch (SQLException e) {
            // do nothing
        }

        final JdbcDataContext dc = new JdbcDataContext(conn);
        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                Table table = callback.createTable(dc.getDefaultSchema(), "test_table").withColumn("foo")
                        .ofType(ColumnType.INTEGER).withColumn("bar").ofType(ColumnType.VARCHAR).execute();
                callback.insertInto(table).value("foo", 1).value("bar", "hello").execute();
                callback.insertInto(table).value("foo", 2).value("bar", "there").execute();
                callback.insertInto(table).value("foo", 3).value("bar", "world").execute();
            }
        });

        Table table = dc.getTableByQualifiedLabel("test_table");
        Query query = new Query().from(table, "a").from(table, "b");
        query.select(table.getColumnByName("foo"), query.getFromClause().getItem(0));
        query.select(table.getColumnByName("foo"), query.getFromClause().getItem(1));
        query.where(new SelectItem(table.getColumnByName("bar"), query.getFromClause().getItem(0)), OperatorType.EQUALS_TO,
                "hello");

        assertEquals(
                "SELECT a.\"FOO\", b.\"FOO\" FROM PUBLIC.\"TEST_TABLE\" a, PUBLIC.\"TEST_TABLE\" b WHERE a.\"BAR\" = 'hello'",
                query.toSql());

        DataSet ds = dc.executeQuery(query);
        assertTrue(ds.next());
        assertEquals("Row[values=[1, 1]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[1, 2]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[1, 3]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.dropTable("test_table").execute();
            }
        });
    }

    private JdbcDataContext createLimitAndOffsetTestData() {
        final JdbcDataContext dc = new JdbcDataContext(conn);

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
                Table table = callback.createTable(dc.getDefaultSchema(), "test_table").withColumn("foo")
                        .ofType(ColumnType.INTEGER).withColumn("bar").ofType(ColumnType.VARCHAR).execute();
                callback.insertInto(table).value("foo", 1).value("bar", "hello").execute();
                callback.insertInto(table).value("foo", 2).value("bar", "there").execute();
                callback.insertInto(table).value("foo", 3).value("bar", "world").execute();
            }
        });

        dc.refreshSchemas();

        return dc;
    }

    public void testLimit() throws Exception {
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
        JdbcDataContext dc = createLimitAndOffsetTestData();
        Schema schema = dc.getDefaultSchema();
        Table productsTable = schema.getTableByName("test_table");

        DataSet ds = dc.query().from(productsTable).select("foo").limit(1).offset(1).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[2]]", ds.getRow().toString());
        assertFalse(ds.next());

        ds.close();
    }

    public void testConvertClobToString() throws Exception {
        JdbcDataContext dc = new JdbcDataContext(conn);

        JdbcTestTemplates.convertClobToString(dc);
    }

    public void testDifferentOperators() throws Exception {
        JdbcTestTemplates.differentOperatorsTest(conn);
    }

    public void testWorkingWithDates() throws Exception {
        final JdbcDataContext dc = new JdbcDataContext(conn);
        final Schema schema = dc.getDefaultSchema();
        JdbcTestTemplates.createInsertAndUpdateDateTypes(dc, schema, "test_table");
    }

    public void testAutomaticConversionWhenInsertingString() throws Exception {
        JdbcTestTemplates.automaticConversionWhenInsertingString(conn);
    }

    public void testCharOfSizeOne() throws Exception {
        JdbcTestTemplates.meaningOfOneSizeChar(conn);
    }
    
    public void testInterpretationOfNull() throws Exception {
        JdbcTestTemplates.interpretationOfNulls(conn);
    }
}