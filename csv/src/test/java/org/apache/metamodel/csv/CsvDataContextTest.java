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
package org.apache.metamodel.csv;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.swing.table.TableModel;

import junit.framework.TestCase;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.convert.Converters;
import org.apache.metamodel.convert.StringToBooleanConverter;
import org.apache.metamodel.convert.StringToIntegerConverter;
import org.apache.metamodel.convert.TypeConverter;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetTableModel;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.naming.CustomColumnNamingStrategy;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.MutableRef;

public class CsvDataContextTest extends TestCase {

    private final CsvConfiguration semicolonConfiguration = new CsvConfiguration(
            CsvConfiguration.DEFAULT_COLUMN_NAME_LINE, "UTF-8", ';', '\'', CsvConfiguration.DEFAULT_ESCAPE_CHAR);

    public void testEmptyFileNoColumnHeaderLine() throws Exception {
        final File file = new File("target/testEmptyFileNoColumnHeaderLine.csv");
        FileHelper.copy(new File("src/test/resources/empty_file.csv"), file);
        
        CsvConfiguration csvConfiguration = new CsvConfiguration(CsvConfiguration.NO_COLUMN_NAME_LINE,
                FileHelper.DEFAULT_ENCODING, CsvConfiguration.DEFAULT_SEPARATOR_CHAR, CsvConfiguration.NOT_A_CHAR,
                CsvConfiguration.DEFAULT_ESCAPE_CHAR);
        final CsvDataContext dc = new CsvDataContext(file, csvConfiguration);
        assertEquals(1, dc.getDefaultSchema().getTableCount());

        dc.executeUpdate(new UpdateScript() {

            @Override
            public void run(UpdateCallback callback) {
                callback.createTable(dc.getDefaultSchema(), "new_table").withColumn("COL_1").withColumn("COL_2")
                        .execute();
                callback.insertInto("new_table").value(0, "1").value(1, 2).execute();
            }
        });
        
        CsvDataContext dc1 = new CsvDataContext(file, csvConfiguration);

        List<Table> tables = dc1.getDefaultSchema().getTables();
        assertEquals(1, tables.size());
        
        Table table = tables.get(0);
        assertEquals("testEmptyFileNoColumnHeaderLine.csv", table.getName());
        assertEquals(2, table.getColumnCount());
        
        DataSet ds = dc1.query().from(table).selectAll().execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1, 2]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();
    }

    public void testEmptyFileTableCreation() throws Exception {
        final File file = new File("target/testEmptyFileNoColumnHeaderLine.csv");
        FileHelper.copy(new File("src/test/resources/empty_file.csv"), file);

        final CsvDataContext dc = new CsvDataContext(file);
        assertEquals(1, dc.getDefaultSchema().getTableCount());

        final Table table1 = dc.getDefaultSchema().getTables().get(0);
        assertEquals("testEmptyFileNoColumnHeaderLine.csv", table1.getName());
        assertEquals(0, table1.getColumnCount());

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.dropTable(dc.getDefaultSchema().getTable(0)).execute();
                callback.createTable(dc.getDefaultSchema(), "newtable1").withColumn("foo").withColumn("bar").execute();
            }
        });

        assertEquals("\"foo\",\"bar\"", FileHelper.readFileAsString(file));

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                // fire additional create table statements
                callback.createTable(dc.getDefaultSchema(), "newtable2").withColumn("foo").withColumn("bar").execute();
                callback.createTable(dc.getDefaultSchema(), "newtable3").withColumn("bar").withColumn("baz").execute();
            }
        });

        assertEquals("\"bar\",\"baz\"", FileHelper.readFileAsString(file));

        // still the table count should only be 1
        assertEquals(1, dc.getDefaultSchema().getTableCount());
    }

    public void testAppendToFileWithoutLineBreak() throws Exception {
        File targetFile = new File("target/csv_no_linebreak");
        FileHelper.copy(new File("src/test/resources/csv_no_linebreak.csv"), targetFile);

        assertTrue(targetFile.exists());

        assertEquals("foo,bar!LINEBREAK!hello,world!LINEBREAK!hi,there", FileHelper.readFileAsString(targetFile)
                .replaceAll("\n", "!LINEBREAK!"));

        final CsvDataContext dc = new CsvDataContext(targetFile);
        final Table table = dc.getDefaultSchema().getTables().get(0);

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value(0, "1234").value(1, "5678").execute();
            }
        });

        assertEquals("foo,bar!LINEBREAK!hello,world!LINEBREAK!hi,there!LINEBREAK!\"1234\",\"5678\"", FileHelper
                .readFileAsString(targetFile).replaceAll("\n", "!LINEBREAK!"));
    }

    public void testHandlingOfEmptyLinesMultipleLinesSupport() throws Exception {
        // test with multiline values
        DataContext dc = new CsvDataContext(new File("src/test/resources/csv_with_empty_lines.csv"),
                new CsvConfiguration(1, false, true));
        testHandlingOfEmptyLines(dc);
    }

    public void testHandlingOfEmptyLinesSingleLinesSupport() throws Exception {
        // test with only single line values
        DataContext dc = new CsvDataContext(new File("src/test/resources/csv_with_empty_lines.csv"),
                new CsvConfiguration(1, false, false));
        testHandlingOfEmptyLines(dc);
    }

    public void testHandlingOfEmptyLines(DataContext dc) throws Exception {
        DataSet ds = dc.query().from(dc.getDefaultSchema().getTable(0)).selectAll().execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[hello, world]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[hi, there]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();
    }

    public void testEmptyFileNoHeaderLine() throws Exception {
        DataContext dc = new CsvDataContext(new File("src/test/resources/empty_file.csv"), new CsvConfiguration(
                CsvConfiguration.NO_COLUMN_NAME_LINE));
        assertEquals(1, dc.getDefaultSchema().getTableCount());

        Table table = dc.getDefaultSchema().getTables().get(0);
        assertEquals("empty_file.csv", table.getName());
        assertEquals(0, table.getColumnCount());
    }

    public void testUnexistingHeaderLine() throws Exception {
        DataContext dc = new CsvDataContext(new File("src/test/resources/csv_people.csv"), new CsvConfiguration(20));
        assertEquals(1, dc.getDefaultSchema().getTableCount());

        Table table = dc.getDefaultSchema().getTables().get(0);
        assertEquals("csv_people.csv", table.getName());
        assertEquals(0, table.getColumnCount());
    }

    public void testInconsistentColumns() throws Exception {
        CsvConfiguration conf = new CsvConfiguration(CsvConfiguration.DEFAULT_COLUMN_NAME_LINE, "UTF8", ',', '"', '\\',
                true);
        DataContext dc = new CsvDataContext(new File("src/test/resources/csv_inconsistent_columns.csv"), conf);
        DataSet ds = dc.query().from("csv_inconsistent_columns.csv").select("hello").and("world").execute();
        assertTrue(ds.next());
        assertTrue(ds.next());

        try {
            ds.next();
            fail("Exception expected");
        } catch (InconsistentRowLengthException e) {
            assertEquals("Inconsistent length of row no. 3. Expected 2 columns but found 3.", e.getMessage());
            Row proposedRow = e.getProposedRow();
            assertEquals("[5, 6]", Arrays.toString(proposedRow.getValues()));

            String[] sourceLine = e.getSourceLine();
            assertEquals("[5, 6, 7]", Arrays.toString(sourceLine));
        }

        assertTrue(ds.next());

        try {
            ds.next();
            fail("Exception expected");
        } catch (InconsistentRowLengthException e) {
            assertEquals("Inconsistent length of row no. 5. Expected 2 columns but found 1.", e.getMessage());
            Row proposedRow = e.getProposedRow();
            assertEquals("[10, null]", Arrays.toString(proposedRow.getValues()));

            String[] sourceLine = e.getSourceLine();
            assertEquals("[10]", Arrays.toString(sourceLine));
        }

        assertTrue(ds.next());
        assertFalse(ds.next());
    }

    public void testApproximatedCountSmallFile() throws Exception {
        DataContext dc = new CsvDataContext(new File("src/test/resources/csv_people.csv"));

        Table table = dc.getDefaultSchema().getTables().get(0);
        Query q = dc.query().from(table).selectCount().toQuery();
        SelectItem selectItem = q.getSelectClause().getItem(0);
        selectItem.setFunctionApproximationAllowed(true);

        DataSet ds = dc.executeQuery(q);
        assertTrue(ds.next());
        Object[] values = ds.getRow().getValues();
        assertEquals(1, values.length);
        assertEquals(9, ((Long) ds.getRow().getValue(selectItem)).intValue());
        assertEquals(9, ((Long) values[0]).intValue());
        assertFalse(ds.next());
    }

    public void testFilterOnNumberColumn() throws Exception {
        CsvDataContext dc = new CsvDataContext(new File("src/test/resources/csv_people.csv"));
        Table table = dc.getDefaultSchema().getTables().get(0);

        Query q = dc.query().from(table).select("name").where("age").greaterThan(18).toQuery();
        List<Object[]> result = dc.executeQuery(q).toObjectArrays();
        assertEquals(2, result.size());
        assertEquals("[michael]", Arrays.toString(result.get(0)));
        assertEquals("[hillary]", Arrays.toString(result.get(1)));
    }

    public void testGetFromInputStream() throws Exception {
        DataContext dc = null;

        // repeat this step a few times to test temp-file creation, see Ticket
        // #437
        for (int i = 0; i < 5; i++) {
            File file = new File("src/test/resources/tickets.csv");
            FileInputStream inputStream = new FileInputStream(file);
            dc = new CsvDataContext(inputStream, new CsvConfiguration());
        }

        Schema schema = dc.getDefaultSchema();
        String name = schema.getTable(0).getName();
        assertTrue(name.startsWith("metamodel"));
        assertTrue(name.endsWith("csv"));

        // Test two seperate reads to ensure that the temp file is working
        // properly and persistent.
        doTicketFileTests(dc);
        doTicketFileTests(dc);
    }

    public void testMultilineExample() throws Exception {
        File file = new File("src/test/resources/tickets.csv");
        DataContext dc = new CsvDataContext(file);
        Schema schema = dc.getDefaultSchema();
        Table table = schema.getTableByName("tickets.csv");
        Column descColumn = table.getColumnByName("_description");

        assertNotNull(table);
        assertNotNull(descColumn);

        doTicketFileTests(dc);
    }

    public void doTicketFileTests(DataContext dc) {
        Table table = dc.getDefaultSchema().getTables().get(0);
        Query q = dc.query().from(table).select(table.getColumns()).toQuery();

        DataSet dataSet = dc.executeQuery(q);
        List<Object[]> objectArrays = dataSet.toObjectArrays();
        assertEquals(13, objectArrays.get(0).length);
        assertEquals(36, objectArrays.size());
        assertEquals("2", objectArrays.get(0)[0].toString());

        Object description = objectArrays.get(0)[11];
        assertTrue(description instanceof String);
        assertEquals(
                "We should have a look at the Value Distribution and Time Analysis profiles. They consume very large amounts of memory because they basicly save all values in maps for analysis.\n"
                        + "\n"
                        + "One way of improving this could be through caching. Another way could be through more appropriate (less verbose) storing of intermediate data (this looks obvious in Time Analysis profile). A third way could be by letting the profiles create queries themselves (related to metadata profiling, #222).",
                (String) description);
    }

    public void testHighColumnNameLineNumber() throws Exception {
        File file = new File("src/test/resources/csv_people.csv");
        QueryPostprocessDataContext dc = new CsvDataContext(file, new CsvConfiguration(3));

        assertEquals(2, dc.getSchemas().size());
        Schema schema = dc.getDefaultSchema();
        assertEquals("resources", schema.getName());
        assertEquals(1, schema.getTableCount());
        Table table = schema.getTables().get(0);
        assertEquals("csv_people.csv", table.getName());

        assertEquals(4, table.getColumnCount());
        assertEquals(0, table.getRelationshipCount());

        Column[] columns = table.getColumns().toArray(new Column[0]);
        assertEquals("2", columns[0].getName());
        assertEquals("michael", columns[1].getName());
        assertEquals("male", columns[2].getName());
        assertEquals("19", columns[3].getName());

        Query query = dc.query().from(table).select(table.getColumnByName("michael")).toQuery();

        DataSet dataSet = dc.executeQuery(query);
        assertTrue(dataSet.next());
        assertEquals("peter", dataSet.getRow().getValue(0));
        assertTrue(dataSet.next());
        assertEquals("bob", dataSet.getRow().getValue(0));
        assertTrue(dataSet.next());
        assertEquals("barbara, barb", dataSet.getRow().getValue(0));
    }

    public void testNoColumnNames() throws Exception {
        File file = new File("src/test/resources/csv_people.csv");
        QueryPostprocessDataContext dc = new CsvDataContext(file, new CsvConfiguration(
                CsvConfiguration.NO_COLUMN_NAME_LINE));
        assertEquals(2, dc.getSchemas().size());
        Schema schema = dc.getDefaultSchema();
        assertEquals("resources", schema.getName());
        assertEquals(1, schema.getTableCount());
        Table table = schema.getTables().get(0);
        assertEquals("csv_people.csv", table.getName());

        assertEquals(4, table.getColumnCount());
        assertEquals(0, table.getRelationshipCount());

        Column[] columns = table.getColumns().toArray(new Column[0]);
        assertEquals("A", columns[0].getName());
        assertEquals("B", columns[1].getName());
        assertEquals("C", columns[2].getName());
        assertEquals("D", columns[3].getName());

        Query query = dc.query().from(table).select(table.getColumnByName("B")).toQuery();

        DataSet dataSet = dc.executeQuery(query);
        assertTrue(dataSet.next());
        assertEquals("name", dataSet.getRow().getValue(0));
        assertTrue(dataSet.next());
        assertEquals("mike", dataSet.getRow().getValue(0));
        assertTrue(dataSet.next());
        assertEquals("michael", dataSet.getRow().getValue(0));
    }

    public void testGetSchemas() throws Exception {
        File file = new File("src/test/resources/csv_people.csv");
        QueryPostprocessDataContext dc = new CsvDataContext(file);
        assertEquals(2, dc.getSchemas().size());
        Schema schema = dc.getDefaultSchema();
        assertEquals("resources", schema.getName());
        assertEquals(1, schema.getTableCount());
        Table table = schema.getTables().get(0);
        assertEquals("csv_people.csv", table.getName());

        assertEquals(4, table.getColumnCount());
        assertEquals(0, table.getRelationshipCount());

        Column[] columns = table.getColumns().toArray(new Column[0]);
        assertEquals("id", columns[0].getName());
        assertEquals("name", columns[1].getName());
        assertEquals("gender", columns[2].getName());
        assertEquals("age", columns[3].getName());
    }

    public void testWhereItemNotInSelectClause() throws Exception {
        File file = new File("src/test/resources/csv_people.csv");
        QueryPostprocessDataContext dc = new CsvDataContext(file);
        Table table = dc.getDefaultSchema().getTableByName("csv_people.csv");

        Query q = new Query();
        q.from(table);
        q.where(new FilterItem(new SelectItem(table.getColumnByName("id")), OperatorType.EQUALS_TO, 1));
        q.select(table.getColumnByName("name"));
        DataSet data = dc.executeQuery(q);
        assertTrue(data.next());
        assertEquals("Row[values=[mike]]", data.getRow().toString());
        assertFalse(data.next());
    }

    public void testWhereColumnInValues() throws Exception {
        File file = new File("src/test/resources/csv_people.csv");
        QueryPostprocessDataContext dc = new CsvDataContext(file, new CsvConfiguration(1, true, true));
        Table table = dc.getDefaultSchema().getTableByName("csv_people.csv");

        Query q = dc.query().from(table).as("t").select("name").and("age").where("age").in("18", "20").toQuery();
        assertEquals("SELECT t.name, t.age FROM resources.csv_people.csv t WHERE t.age IN ('18' , '20')", q.toSql());

        DataSet ds = dc.executeQuery(q);
        assertTrue(ds.next());
        assertEquals("Row[values=[mike, 18]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[peter, 18]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[barbara, barb, 18]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[charlotte, 18]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[hillary, 20]]", ds.getRow().toString());
        assertFalse(ds.next());

        ds.close();
    }

    public void testGroupByQuery() throws Exception {
        DataContext dc = new CsvDataContext(new File("src/test/resources/csv_people.csv"));
        Table table = dc.getDefaultSchema().getTableByName("csv_people.csv");

        Query q = new Query();
        q.from(table);
        q.groupBy(table.getColumnByName("gender"));
        q.select(new SelectItem(table.getColumnByName("gender")),
                new SelectItem(FunctionType.MAX, table.getColumnByName("age")),
                new SelectItem(FunctionType.MIN, table.getColumnByName("age")), new SelectItem(FunctionType.COUNT, "*",
                        "total"), new SelectItem(FunctionType.MIN, table.getColumnByName("id")).setAlias("firstId"));
        DataSet data = dc.executeQuery(q);
        assertEquals(
                "[csv_people.csv.gender, MAX(csv_people.csv.age), MIN(csv_people.csv.age), COUNT(*) AS total, MIN(csv_people.csv.id) AS firstId]",
                Arrays.toString(data.getSelectItems().toArray()));

        String[] expectations = new String[] { "Row[values=[female, 20, 17, 5, 5]]", "Row[values=[male, 19, 17, 4, 1]]" };

        assertTrue(data.next());
        assertTrue(Arrays.asList(expectations).contains(data.getRow().toString()));
        assertTrue(data.next());
        assertTrue(Arrays.asList(expectations).contains(data.getRow().toString()));
        assertFalse(data.next());
    }

    public void testMaterializeTable() throws Exception {
        File file = new File("src/test/resources/csv_people.csv");
        CsvDataContext dc = new CsvDataContext(file, new CsvConfiguration(1, false, false));
        Table table = dc.getSchemas().get(0).getTables().get(0);
        DataSet dataSet = dc.materializeMainSchemaTable(table, table.getColumns(), -1);
        assertNull(dataSet.getRow());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[1, mike, male, 18]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[2, michael, male, 19]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[3, peter, male, 18]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[5, barbara, barb, female, 18]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[9, carrie, female, 17]]", dataSet.getRow().toString());
        assertFalse(dataSet.next());

        dataSet = dc.materializeMainSchemaTable(table, table.getColumns(), 1);
        assertTrue(dataSet.next());
        assertEquals("Row[values=[1, mike, male, 18]]", dataSet.getRow().toString());
        assertFalse(dataSet.next());
    }

    public void testAlternativeDelimitors() throws Exception {
        File file = new File("src/test/resources/csv_semicolon_singlequote.csv");
        CsvDataContext dc = new CsvDataContext(file, semicolonConfiguration);
        Table table = dc.getSchemas().get(0).getTables().get(0);
        DataSet dataSet = dc.materializeMainSchemaTable(table, table.getColumns(), -1);
        assertTrue(dataSet.next());
        assertEquals("Row[values=[1, mike, male, 18]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[2, michael, male, 19]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[3, peter, male, 18]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[5, barbara; barb, female, 18]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[9, carrie, female, 17]]", dataSet.getRow().toString());
        assertFalse(dataSet.next());
        assertNull(dataSet.getRow());
    }

    public void testMaxRows() throws Exception {
        File file = new File("src/test/resources/csv_semicolon_singlequote.csv");
        CsvDataContext dc = new CsvDataContext(file, semicolonConfiguration);
        Table table = dc.getDefaultSchema().getTables().get(0);
        Query query = new Query().from(table).select(table.getColumns()).setMaxRows(5);
        DataSet dataSet = dc.executeQuery(query);

        TableModel tableModel = new DataSetTableModel(dataSet);
        assertEquals(5, tableModel.getRowCount());
    }

    public void testQueryOnlyAggregate() throws Exception {
        File file = new File("src/test/resources/csv_people.csv");
        QueryPostprocessDataContext dc = new CsvDataContext(file);
        Table table = dc.getDefaultSchema().getTables().get(0);

        Query q = new Query().selectCount().from(table);
        assertEquals("SELECT COUNT(*) FROM resources.csv_people.csv", q.toString());

        List<Object[]> data = dc.executeQuery(q).toObjectArrays();
        assertEquals(1, data.size());
        Object[] row = data.get(0);
        assertEquals(1, row.length);
        assertEquals("[9]", Arrays.toString(row));

        q.select(table.getColumns().get(0));
        assertEquals("SELECT COUNT(*), csv_people.csv.id FROM resources.csv_people.csv", q.toString());
        data = dc.executeQuery(q).toObjectArrays();
        assertEquals(9, data.size());
        row = data.get(0);
        assertEquals(2, row.length);
        assertEquals("[9, 1]", Arrays.toString(row));

        row = data.get(1);
        assertEquals(2, row.length);
        assertEquals("[9, 2]", Arrays.toString(row));

        row = data.get(2);
        assertEquals(2, row.length);
        assertEquals("[9, 3]", Arrays.toString(row));

        row = data.get(8);
        assertEquals(2, row.length);
        assertEquals("[9, 9]", Arrays.toString(row));
    }

    public void testOffsetAndMaxrows() throws Exception {
        DataContext dc = new CsvDataContext(new File("src/test/resources/csv_people.csv"));

        Table table = dc.getDefaultSchema().getTables().get(0);
        Query q = dc.query().from(table).select(table.getColumnByName("name")).toQuery();
        q.setFirstRow(3);
        q.setMaxRows(2);

        DataSet ds;

        ds = dc.executeQuery(q);
        assertEquals(1, ds.getSelectItems().size());
        assertTrue(ds.next());
        assertEquals("peter", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("bob", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        // try with iterator
        ds = dc.executeQuery(q);
        int i = 0;
        for (Row row : ds) {
            assertNotNull(row);
            i++;
        }
        assertEquals(2, i);
    }

    public void testTruncateDeleteAllRecordsFromInconsistentFile() throws Exception {
        File file = new File("target/csv_delete_all_records.txt");
        FileHelper.copy(new File("src/test/resources/csv_to_be_truncated.csv"), file);

        CsvDataContext dc = new CsvDataContext(file, new CsvConfiguration(1, "UTF8", ',', '"', '\\', true));
        assertEquals("[id, name, gender, age]", Arrays.toString(dc.getDefaultSchema().getTable(0).getColumnNames().toArray()));

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.deleteFrom("csv_delete_all_records.txt").execute();
            }
        });

        DataSet ds = dc.query().from("csv_delete_all_records.txt").selectCount().execute();
        assertTrue(ds.next());
        assertEquals(0, ((Number) ds.getRow().getValue(0)).intValue());
        assertFalse(ds.next());

        String fileAsString = FileHelper.readFileAsString(file);
        assertEquals("\"id\",\"name\",\"gender\",\"age\"", fileAsString);
    }

    public void testWriteSimpleTableInNewFile() throws Exception {
        final File file = new File("target/csv_write_ex1.txt");
        file.delete();
        assertFalse(file.exists());
        CsvDataContext dc = new CsvDataContext(file);
        final Schema schema = dc.getDefaultSchema();
        assertEquals(0, schema.getTableCount());

        final MutableRef<Table> tableRef = new MutableRef<Table>();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                Table table = cb.createTable(schema, "foobar").withColumn("foo").withColumn("bar").execute();
                tableRef.set(table);
                assertEquals(schema, table.getSchema());
                assertEquals(schema.getTables().get(0), table);
                assertTrue(file.exists());

                assertEquals("[foo, bar]", Arrays.toString(table.getColumnNames().toArray()));

                cb.insertInto(table).value(0, "f").value(1, "b").execute();
                cb.insertInto(table).value(0, "o").value(table.getColumnByName("bar"), "a").execute();
                cb.insertInto(table).value(0, "o").value("bar", "r").execute();
            }
        });

        // query the file to check results
        final Table readTable = schema.getTables().get(0);
        assertEquals(tableRef.get(), readTable);
        assertEquals("[foo, bar]", Arrays.toString(readTable.getColumnNames().toArray()));

        final Query query = dc.query().from(readTable).select("bar").and("foo").toQuery();
        DataSet ds = dc.executeQuery(query);

        assertTrue(ds.next());
        assertEquals("Row[values=[b, f]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[a, o]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[r, o]]", ds.getRow().toString());
        assertFalse(ds.next());

        // do the same trick on an existing file
        dc = new CsvDataContext(file);
        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                cb.insertInto(tableRef.get()).value("foo", "hello").value("bar", "world").execute();
            }
        });

        ds = dc.executeQuery(query);

        assertTrue(ds.next());
        assertEquals("Row[values=[b, f]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[a, o]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[r, o]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[world, hello]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.deleteFrom(readTable).where("bar").eq("a").execute();
                callback.deleteFrom(readTable).where("bar").eq("r").execute();
            }
        });

        ds = dc.executeQuery(query);
        assertTrue(ds.next());
        assertEquals("Row[values=[b, f]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[world, hello]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.update(readTable).value("foo", "universe").execute();
                callback.update(readTable).value("bar", "c").where("bar").isEquals("b").execute();
            }
        });

        ds = dc.executeQuery(query);
        assertTrue(ds.next());
        assertEquals("Row[values=[world, universe]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[c, universe]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        // drop table
        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.dropTable(readTable).execute();
            }
        });

        assertFalse(file.exists());
    }

    public void testOnlyNumberOneSymbol() throws Exception {
        DataContext dc = new CsvDataContext(new File("src/test/resources/csv_only_number_one.csv"));
        Map<Column, TypeConverter<?, ?>> converters = Converters.autoDetectConverters(dc, dc.getDefaultSchema()
                .getTables().get(0), 1000);

        assertEquals(1, converters.size());
        assertEquals(StringToBooleanConverter.class, converters.values().iterator().next().getClass());

        dc = Converters.addTypeConverters(dc, converters);

        Table table = dc.getDefaultSchema().getTables().get(0);
        MutableColumn col = (MutableColumn) table.getColumns().get(0);
        Query q = dc.query().from(table).select(col).toQuery();
        assertEquals("SELECT csv_only_number_one.csv.number FROM resources.csv_only_number_one.csv", q.toSql());

        DataSet ds = dc.executeQuery(q);
        while (ds.next()) {
            assertEquals(true, ds.getRow().getValue(0));
        }
        ds.close();

        dc = Converters.addTypeConverter(dc, col, new StringToIntegerConverter());

        ds = dc.executeQuery(q);
        while (ds.next()) {
            assertEquals(1, ds.getRow().getValue(0));
        }
        ds.close();
    }

    public void testWriteOddConfiguration() throws Exception {
        final File file = new File("target/csv_write_ex2.txt");
        file.delete();
        assertFalse(file.exists());

        final CsvDataContext dc = new CsvDataContext(file, new CsvConfiguration(
                CsvConfiguration.DEFAULT_COLUMN_NAME_LINE, "UTF8", '|', '?', '!'));
        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                Table table = cb.createTable(dc.getDefaultSchema(), "table").withColumn("id").withColumn("name")
                        .execute();
                cb.insertInto(table).value("id", 1).value("name", "Kasper").execute();
                cb.insertInto(table).value("id", 2).value("name", "Kas|per?").execute();
            }
        });

        String[] lines = FileHelper.readFileAsString(file).split("\n");
        assertEquals(3, lines.length);
        assertEquals("?id?|?name?", lines[0]);
        assertEquals("?1?|?Kasper?", lines[1]);
        assertEquals("?2?|?Kas|per!??", lines[2]);
    }

    public void testCannotWriteToReadOnly() throws Exception {
        final CsvDataContext dc = new CsvDataContext(new FileInputStream("src/test/resources/empty_file.csv"),
                new CsvConfiguration());
        try {
            dc.executeUpdate(new UpdateScript() {
                @Override
                public void run(UpdateCallback cb) {
                    cb.createTable(dc.getDefaultSchema(), "foo");
                }
            });
            fail("Exception expected");
        } catch (IllegalStateException e) {
            assertEquals("This CSV DataContext is not writable, as it based on a read-only resource.", e.getMessage());
        }

        // try {
        // dc.executeUpdate(new Update() {
        // @Override
        // public void run(UpdateCallback cb) {
        // cb.insertInto(dc.getDefaultSchema().getTables()[0]);
        // }
        // });
        // fail("Exception expected");
        // } catch (IllegalStateException e) {
        // assertEquals(
        // "This CSV DataContext is not writable, as it based on a read-only resource.",
        // e.getMessage());
        // }
    }

    // public void testOnlyWriteToOwnSchemasAndTables() throws Exception {
    // CsvDataContext dc = new CsvDataContext(new File(
    // "src/test/resources/empty_file.csv"), new CsvConfiguration());
    // try {
    // dc.executeUpdate(new Update() {
    // @Override
    // public void run(UpdateCallback cb) {
    // cb.createTable(new MutableSchema("bar"), "foo");
    // }
    // );
    // fail("Exception expected");
    // } catch (IllegalArgumentException e) {
    // assertEquals("Not a valid CSV schema: Schema[name=bar]",
    // e.getMessage());
    // }
    //
    // try {
    // dc.insertInto(new MutableTable("bla"));
    // fail("Exception expected");
    // } catch (IllegalArgumentException e) {
    // assertEquals(
    // "Not a valid CSV table: Table[name=bla,type=null,remarks=null]",
    // e.getMessage());
    // }
    // }

    public void testCustomColumnNames() throws Exception {
        final String firstColumnName = "first";
        final String secondColumnName = "second";
        final String thirdColumnName = "third";
        final String fourthColumnName = "fourth";

        final CsvConfiguration configuration = new CsvConfiguration(CsvConfiguration.DEFAULT_COLUMN_NAME_LINE,
                new CustomColumnNamingStrategy(firstColumnName, secondColumnName, thirdColumnName, fourthColumnName),
                FileHelper.DEFAULT_ENCODING, CsvConfiguration.DEFAULT_SEPARATOR_CHAR,
                CsvConfiguration.DEFAULT_QUOTE_CHAR, CsvConfiguration.DEFAULT_ESCAPE_CHAR, false, true);

        final DataContext dataContext = new CsvDataContext(new File("src/test/resources/csv_people.csv"),
                configuration);

        final Table table = dataContext.getDefaultSchema().getTable(0);

        assertNotNull(table.getColumnByName(firstColumnName));
        assertNotNull(table.getColumnByName(secondColumnName));
        assertNotNull(table.getColumnByName(thirdColumnName));
        assertNotNull(table.getColumnByName(fourthColumnName));
    }
}