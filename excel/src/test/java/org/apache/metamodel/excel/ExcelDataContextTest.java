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
package org.apache.metamodel.excel;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.data.StyleBuilder;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.naming.CustomColumnNamingStrategy;
import org.apache.metamodel.util.DateUtils;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.Month;

import junit.framework.TestCase;

public class ExcelDataContextTest extends TestCase {

    /**
     * Creates a copy of a particular file - to avoid changing of Excel files
     * under source control
     * 
     * @param path
     * @return
     */
    private File copyOf(String path) {
        final File srcFile = new File(path);
        final File destFile = new File("target/" + getName() + "-" + srcFile.getName());
        FileHelper.copy(srcFile, destFile);
        return destFile;
    }

    public void testErrornousConstructors() throws Exception {
        try {
            new ExcelDataContext(null);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertEquals("File cannot be null", e.getMessage());
        }

        File file = copyOf("src/test/resources/empty_file.xls");
        try {
            new ExcelDataContext(file, null);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertEquals("ExcelConfiguration cannot be null", e.getMessage());
        }
    }

    public void testEmptyFile() throws Exception {
        File file = copyOf("src/test/resources/empty_file.xls");
        ExcelDataContext dc = new ExcelDataContext(file);

        assertNull(dc.getSpreadsheetReaderDelegateClass());
        assertEquals(1, dc.getDefaultSchema().getTableCount());

        Table table = dc.getDefaultSchema().getTables()[0];
        assertEquals("sheet", table.getName());
        assertEquals(0, table.getColumnCount());

        assertSame(file, ((FileResource) dc.getResource()).getFile());
    }

    public void testEmptyFileNoHeaderLine() throws Exception {
        DataContext dc = new ExcelDataContext(copyOf("src/test/resources/empty_file.xls"), new ExcelConfiguration(
                ExcelConfiguration.NO_COLUMN_NAME_LINE, false, false));
        assertEquals(1, dc.getDefaultSchema().getTableCount());

        Table table = dc.getDefaultSchema().getTables()[0];
        assertEquals("sheet", table.getName());
        assertEquals(0, table.getColumnCount());
    }

    public void testUnexistingHeaderLine() throws Exception {
        DataContext dc = new ExcelDataContext(copyOf("src/test/resources/xls_people.xls"), new ExcelConfiguration(20,
                true, false));
        assertEquals(1, dc.getDefaultSchema().getTableCount());

        Table table = dc.getDefaultSchema().getTables()[0];
        assertEquals("xls_people", table.getName());
        assertEquals(0, table.getColumnCount());
    }

    public void testSkipEmptyColumns() throws Exception {
        ExcelConfiguration conf = new ExcelConfiguration(ExcelConfiguration.DEFAULT_COLUMN_NAME_LINE, true, true);
        ExcelDataContext dc = new ExcelDataContext(copyOf("src/test/resources/skipped_lines.xlsx"), conf);
        Table table = dc.getDefaultSchema().getTables()[0];
        assertEquals("[hello, world]", Arrays.toString(table.getColumnNames()));

        DataSet ds = dc.executeQuery(dc.query().from(table).select("hello").toQuery());
        assertTrue(ds.next());
        assertEquals("1", ds.getRow().getValue(0));
    }

    public void testDontSkipEmptyLinesNoHeader() throws Exception {
        ExcelConfiguration conf = new ExcelConfiguration(ExcelConfiguration.NO_COLUMN_NAME_LINE, false, true);
        ExcelDataContext dc = new ExcelDataContext(copyOf("src/test/resources/skipped_lines.xlsx"), conf);
        Table table = dc.getDefaultSchema().getTables()[0];
        assertEquals("[G, H]", Arrays.toString(table.getColumnNames()));

        assertEquals(6, table.getColumnByName("G").getColumnNumber());
        assertEquals(7, table.getColumnByName("H").getColumnNumber());

        DataSet ds = dc.executeQuery(dc.query().from(table).select("G").toQuery());

        // 5 empty lines
        for (int i = 0; i < 5; i++) {
            assertTrue(ds.next());
            Object value = ds.getRow().getValue(0);
            assertNull("Values was: " + value + " at row " + i, value);
        }

        assertTrue(ds.next());
        assertEquals("hello", ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals("1", ds.getRow().getValue(0));
    }

    public void testDontSkipEmptyLinesAbsoluteHeader() throws Exception {
        ExcelConfiguration conf = new ExcelConfiguration(6, false, true);
        ExcelDataContext dc = new ExcelDataContext(copyOf("src/test/resources/skipped_lines.xlsx"), conf);
        Table table = dc.getDefaultSchema().getTables()[0];
        assertEquals("[hello, world]", Arrays.toString(table.getColumnNames()));
        assertEquals(6, table.getColumnByName("hello").getColumnNumber());
        assertEquals(7, table.getColumnByName("world").getColumnNumber());

        DataSet ds = dc.executeQuery(dc.query().from(table).select("hello").toQuery());
        assertTrue(ds.next());
        assertEquals("1", ds.getRow().getValue(0));
    }

    public void testInvalidFormula() throws Exception {
        ExcelDataContext dc = new ExcelDataContext(copyOf("src/test/resources/invalid_formula.xls"));
        Table table = dc.getDefaultSchema().getTables()[0];

        assertEquals("[name]", Arrays.toString(table.getColumnNames()));

        Query q = dc.query().from(table).select("name").toQuery();

        DataSet ds = dc.executeQuery(dc.query().from(table).selectCount().toQuery());
        assertTrue(ds.next());
        assertEquals(3, Integer.parseInt(ds.getRow().getValue(0).toString()));
        assertFalse(ds.next());
        assertFalse(ds.next());
        ds.close();

        ds = dc.executeQuery(q);

        Row row;

        assertTrue(ds.next());
        row = ds.getRow();
        assertEquals("TismmerswerskisMFSTLandsmeers                                                          ", row
                .getValue(0).toString());

        assertTrue(ds.next());
        row = ds.getRow();
        assertEquals("-\"t\" \"houetismfsthueiss\"", row.getValue(0).toString());

        assertTrue(ds.next());
        row = ds.getRow();
        assertEquals("TismmerswerskisMFSTLandsmeers                                                          ", row
                .getValue(0).toString());

        assertFalse(ds.next());
        ds.close();
    }

    public void testEvaluateFormula() throws Exception {
        ExcelDataContext dc = new ExcelDataContext(copyOf("src/test/resources/xls_formulas.xls"));

        Table table = dc.getDefaultSchema().getTables()[0];
        Column[] columns = table.getColumns();

        assertEquals("[some number, some mixed formula, some int only formula]",
                Arrays.toString(table.getColumnNames()));

        Query q = dc.query().from(table).select(columns).toQuery();
        DataSet ds = dc.executeQuery(q);
        Object value;

        assertTrue(ds.next());
        assertEquals("1", ds.getRow().getValue(columns[0]));
        value = ds.getRow().getValue(columns[1]);
        assertEquals(String.class, value.getClass());
        assertEquals("1", value);

        value = ds.getRow().getValue(columns[2]);
        assertEquals(String.class, value.getClass());
        assertEquals("1", value);

        assertTrue(ds.next());
        assertEquals("2", ds.getRow().getValue(columns[0]));
        value = ds.getRow().getValue(columns[1]);
        assertEquals(String.class, value.getClass());
        assertEquals("3", value);

        value = ds.getRow().getValue(columns[2]);
        assertEquals(String.class, value.getClass());
        assertEquals("3", value);

        assertTrue(ds.next());
        assertEquals("3", ds.getRow().getValue(columns[0]));
        value = ds.getRow().getValue(columns[1]);
        assertEquals(String.class, value.getClass());
        assertEquals("8", value);

        value = ds.getRow().getValue(columns[2]);
        assertEquals(String.class, value.getClass());
        assertEquals("8", value);

        assertTrue(ds.next());
        assertEquals("4", ds.getRow().getValue(columns[0]));
        value = ds.getRow().getValue(columns[1]);
        assertEquals(String.class, value.getClass());
        assertEquals("12", value);

        value = ds.getRow().getValue(columns[2]);
        assertEquals(String.class, value.getClass());
        assertEquals("12", value);

        assertTrue(ds.next());
        assertEquals("5", ds.getRow().getValue(columns[0]));
        value = ds.getRow().getValue(columns[1]);
        assertEquals(String.class, value.getClass());
        assertEquals("yes", value);

        value = ds.getRow().getValue(columns[2]);
        assertEquals(String.class, value.getClass());
        assertEquals("5", value);

        assertTrue(ds.next());
        assertEquals("6", ds.getRow().getValue(columns[0]));
        value = ds.getRow().getValue(columns[1]);
        assertEquals(String.class, value.getClass());
        assertEquals("no", value);

        value = ds.getRow().getValue(columns[2]);
        assertEquals(String.class, value.getClass());
        assertEquals("6", value);

        assertFalse(ds.next());
    }

    public void testSingleCellSheet() throws Exception {
        ExcelDataContext dc = new ExcelDataContext(copyOf("src/test/resources/xls_single_cell_sheet.xls"));

        Table table = dc.getDefaultSchema().getTableByName("Sheet1");

        assertNotNull(table);

        assertEquals("[A, hello]", Arrays.toString(table.getColumnNames()));

        Query q = dc.query().from(table).select(table.getColumns()).toQuery();
        DataSet ds = dc.executeQuery(q);
        assertFalse(ds.next());
    }

    public void testOpenXlsxFormat() throws Exception {
        ExcelDataContext dc = new ExcelDataContext(copyOf("src/test/resources/Spreadsheet2007.xlsx"));
        Schema schema = dc.getDefaultSchema();
        assertEquals("Schema[name=testOpenXlsxFormat-Spreadsheet2007.xlsx]", schema.toString());

        assertEquals("[Sheet1, Sheet2, Sheet3]", Arrays.toString(schema.getTableNames()));

        assertEquals(0, schema.getTableByName("Sheet2").getColumnCount());
        assertEquals(0, schema.getTableByName("Sheet3").getColumnCount());

        Table table = schema.getTableByName("Sheet1");

        assertEquals("[string, number, date]", Arrays.toString(table.getColumnNames()));

        Query q = dc.query().from(table).select(table.getColumns()).orderBy(table.getColumnByName("number")).toQuery();
        DataSet ds = dc.executeQuery(q);
        List<Object[]> objectArrays = ds.toObjectArrays();
        assertEquals(4, objectArrays.size());
        assertEquals("[hello, 1, 2010-01-01 00:00:00]", Arrays.toString(objectArrays.get(0)));
        assertEquals("[world, 2, 2010-01-02 00:00:00]", Arrays.toString(objectArrays.get(1)));
        assertEquals("[foo, 3, 2010-01-03 00:00:00]", Arrays.toString(objectArrays.get(2)));
        assertEquals("[bar, 4, 2010-01-04 00:00:00]", Arrays.toString(objectArrays.get(3)));
    }

    public void testConfigurationWithoutHeader() throws Exception {
        File file = copyOf("src/test/resources/xls_people.xls");
        DataContext dc = new ExcelDataContext(file, new ExcelConfiguration(ExcelConfiguration.NO_COLUMN_NAME_LINE,
                true, true));
        Table table = dc.getDefaultSchema().getTables()[0];

        String[] columnNames = table.getColumnNames();
        assertEquals("[A, B, C, D]", Arrays.toString(columnNames));

        Query q = dc.query().from(table).select(table.getColumnByName("A")).toQuery();
        assertEquals("SELECT xls_people.A FROM testConfigurationWithoutHeader-xls_people.xls.xls_people", q.toSql());

        DataSet dataSet = dc.executeQuery(q);
        assertTrue(dataSet.next());
        assertEquals("id", dataSet.getRow().getValue(0));
        for (int i = 1; i <= 9; i++) {
            assertTrue(dataSet.next());
            assertEquals(i + "", dataSet.getRow().getValue(0));
        }

        assertFalse(dataSet.next());
    }

    public void testConfigurationNonDefaultColumnNameLineNumber() throws Exception {
        File file = copyOf("src/test/resources/xls_people.xls");
        DataContext dc = new ExcelDataContext(file, new ExcelConfiguration(2, true, true));
        Table table = dc.getDefaultSchema().getTables()[0];

        String[] columnNames = table.getColumnNames();
        assertEquals("[1, mike, male, 18]", Arrays.toString(columnNames));

        Query q = dc.query().from(table).select(table.getColumnByName("1")).toQuery();
        assertEquals("SELECT xls_people.1 FROM testConfigurationNonDefaultColumnNameLineNumber-xls_people.xls.xls_people", q.toSql());

        DataSet dataSet = dc.executeQuery(q);
        assertTrue(dataSet.next());
        assertEquals("2", dataSet.getRow().getValue(0));
        for (int i = 3; i <= 9; i++) {
            assertTrue(dataSet.next());
            assertEquals(i + "", dataSet.getRow().getValue(0));
        }
        assertFalse(dataSet.next());
    }

    public void testGetSchemas() throws Exception {
        File file = copyOf("src/test/resources/xls_people.xls");
        DataContext dc = new ExcelDataContext(file);
        Schema[] schemas = dc.getSchemas();
        assertEquals(2, schemas.length);
        Schema schema = schemas[1];
        assertEquals("testGetSchemas-xls_people.xls", schema.getName());
        assertEquals(1, schema.getTableCount());
        Table table = schema.getTables()[0];
        assertEquals("xls_people", table.getName());

        assertEquals(4, table.getColumnCount());
        assertEquals(0, table.getRelationshipCount());

        Column[] columns = table.getColumns();
        assertEquals("id", columns[0].getName());
        assertEquals("name", columns[1].getName());
        assertEquals("gender", columns[2].getName());
        assertEquals("age", columns[3].getName());
    }

    public void testMaterializeTable() throws Exception {
        File file = copyOf("src/test/resources/xls_people.xls");
        ExcelDataContext dc = new ExcelDataContext(file);
        Table table = dc.getDefaultSchema().getTables()[0];
        DataSet dataSet = dc.materializeMainSchemaTable(table, table.getColumns(), -1);
        assertTrue(dataSet.next());
        assertEquals("Row[values=[1, mike, male, 18]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[2, michael, male, 19]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[3, peter, male, 18]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[9, carrie, female, 17]]", dataSet.getRow().toString());
        assertFalse(dataSet.next());
        assertNull(dataSet.getRow());
    }

    public void testMissingValues() throws Exception {
        File file = copyOf("src/test/resources/xls_missing_values.xls");
        DataContext dc = new ExcelDataContext(file);
        Schema schema = dc.getDefaultSchema();
        assertEquals(1, schema.getTableCount());

        Table table = schema.getTables()[0];
        assertEquals("[Column[name=a,columnNumber=0,type=VARCHAR,nullable=true,nativeType=null,columnSize=null], "
                + "Column[name=b,columnNumber=1,type=VARCHAR,nullable=true,nativeType=null,columnSize=null], "
                + "Column[name=c,columnNumber=2,type=VARCHAR,nullable=true,nativeType=null,columnSize=null], "
                + "Column[name=d,columnNumber=3,type=VARCHAR,nullable=true,nativeType=null,columnSize=null]]",
                Arrays.toString(table.getColumns()));

        Query q = new Query().select(table.getColumns()).from(table);
        DataSet ds = dc.executeQuery(q);
        assertTrue(ds.next());
        assertEquals("[1, 2, 3, null]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[5, null, 7, 8]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[9, 10, 11, 12]", Arrays.toString(ds.getRow().getValues()));
        assertFalse(ds.next());
    }

    public void testMissingColumnHeader() throws Exception {
        File file = copyOf("src/test/resources/xls_missing_column_header.xls");
        DataContext dc = new ExcelDataContext(file);
        Schema schema = dc.getDefaultSchema();
        assertEquals(1, schema.getTableCount());

        Table table = schema.getTables()[0];
        assertEquals("[Column[name=a,columnNumber=0,type=VARCHAR,nullable=true,nativeType=null,columnSize=null], "
                + "Column[name=b,columnNumber=1,type=VARCHAR,nullable=true,nativeType=null,columnSize=null], "
                + "Column[name=A,columnNumber=2,type=VARCHAR,nullable=true,nativeType=null,columnSize=null], "
                + "Column[name=d,columnNumber=3,type=VARCHAR,nullable=true,nativeType=null,columnSize=null]]",
                Arrays.toString(table.getColumns()));

        Query q = new Query().select(table.getColumns()).from(table);
        DataSet ds = dc.executeQuery(q);
        assertTrue(ds.next());
        assertEquals("[1, 2, 3, 4]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[5, 6, 7, 8]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[9, 10, 11, 12]", Arrays.toString(ds.getRow().getValues()));
        assertFalse(ds.next());
    }

    public void testXlsxFormulas() throws Exception {
        File file = copyOf("src/test/resources/formulas.xlsx");
        ExcelDataContext dc = new ExcelDataContext(file);

        assertEquals("[sh1]", Arrays.toString(dc.getDefaultSchema().getTableNames()));
        assertEquals(XlsxSpreadsheetReaderDelegate.class, dc.getSpreadsheetReaderDelegateClass());

        Table table = dc.getDefaultSchema().getTableByName("sh1");
        assertEquals("[Foo, Bar]", Arrays.toString(table.getColumnNames()));

        Query q = dc.query().from(table).select("Foo").toQuery();
        DataSet ds = dc.executeQuery(q);

        assertTrue(ds.next());
        assertEquals("1", ds.getRow().getValue(0).toString());
        assertEquals("", ds.getRow().getStyle(0).toString());
        assertTrue(ds.next());
        assertEquals("2", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("3", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("4", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("5", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("6", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("7", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("8", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("9", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("10", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("11", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("12", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("13", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());

        q = dc.query().from(table).select("Bar").toQuery();
        ds = dc.executeQuery(q);

        assertTrue(ds.next());
        assertEquals("lorem", ds.getRow().getValue(0).toString());
        assertEquals("", ds.getRow().getStyle(0).toString());
        assertTrue(ds.next());
        assertEquals("ipsum", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("21", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("foo", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("bar", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("baz", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals(null, ds.getRow().getValue(0));
        assertNotNull(null, ds.getRow().getStyle(0));
        assertTrue(ds.next());
        assertEquals("!\"#¤%&/()<>=?", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("here are", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("some invalid", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("formulas:", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("#DIV/0!", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("0", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
    }

    public void testTicket99defect() throws Exception {
        File file = copyOf("src/test/resources/ticket_199_inventory.xls");
        DataContext dc = new ExcelDataContext(file);
        Schema schema = dc.getDefaultSchema();
        assertEquals(
                "[Table[name=Sheet1,type=null,remarks=null], Table[name=Sheet2,type=null,remarks=null], Table[name=Sheet3,type=null,remarks=null]]",
                Arrays.toString(schema.getTables()));

        assertEquals(0, schema.getTableByName("Sheet2").getColumnCount());
        assertEquals(0, schema.getTableByName("Sheet3").getColumnCount());

        Table table = schema.getTableByName("Sheet1");
        assertEquals(

                "[Column[name=Pkg No.,columnNumber=0,type=VARCHAR,nullable=true,nativeType=null,columnSize=null], "
                        + "Column[name=Description,columnNumber=1,type=VARCHAR,nullable=true,nativeType=null,columnSize=null], "
                        + "Column[name=Room,columnNumber=2,type=VARCHAR,nullable=true,nativeType=null,columnSize=null], "
                        + "Column[name=Level,columnNumber=3,type=VARCHAR,nullable=true,nativeType=null,columnSize=null]]",
                Arrays.toString(table.getColumns()));
    }

    public void testInsertInto() throws Exception {
        final File file = copyOf("src/test/resources/xls_people.xls");

        final ExcelDataContext dc = new ExcelDataContext(file);
        final Table table = dc.getDefaultSchema().getTables()[0];
        final Column nameColumn = table.getColumnByName("name");

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                Style clownStyle = new StyleBuilder().bold().foreground(255, 0, 0).background(0, 0, 255).create();

                Style thirtyStyle = new StyleBuilder().italic().underline().centerAligned().foreground(10, 10, 200)
                        .create();

                cb.insertInto(table).value("id", 1000).value(nameColumn, "pennywise the [clown]", clownStyle)
                        .value("gender", "male").value("age", 30, thirtyStyle).execute();
            }
        });

        DataSet ds = dc.query().from(table).select(nameColumn).orderBy(nameColumn).execute();
        assertTrue(ds.next());
        assertEquals("barbara", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("bob", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("carrie", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("charlotte", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("hillary", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("michael", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("mike", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("pennywise the [clown]", ds.getRow().getValue(0).toString());
        assertEquals("font-weight: bold;color: rgb(255,0,0);background-color: rgb(0,0,255);", ds.getRow().getStyle(0)
                .toString());
        assertTrue(ds.next());
        assertEquals("peter", ds.getRow().getValue(0).toString());
        assertTrue(ds.next());
        assertEquals("vera", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        ds = dc.query().from(table).select("age").where("age").eq(30).execute();
        assertTrue(ds.next());
        assertEquals("30", ds.getRow().getValue(0));
        assertEquals("font-style: italic;text-decoration: underline;text-align: center;color: rgb(0,0,255);", ds
                .getRow().getStyle(0).toCSS());
        assertFalse(ds.next());
    }

    public void testCreateTableXls() throws Exception {
        // run the same test with both XLS and XLSX (because of different
        // workbook implementations)
        runCreateTableTest(new File("target/xls_people_created.xls"));
    }

    public void testCreateTableXlsx() throws Exception {
        // run the same test with both XLS and XLSX (because of different
        // workbook implementations)
        runCreateTableTest(new File("target/xls_people_created.xlsx"));
    }

    private void runCreateTableTest(File file) {
        if (file.exists()) {
            assertTrue(file.delete());
        }
        final ExcelDataContext dc = new ExcelDataContext(file);
        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                Schema schema = dc.getDefaultSchema();
                Table table1 = cb.createTable(schema, "my_table_1").withColumn("foo").withColumn("bar")
                        .withColumn("baz").execute();

                assertEquals(1, schema.getTableCount());
                assertSame(table1.getSchema(), schema);
                assertSame(table1, schema.getTables()[0]);

                Table table2 = cb.createTable(schema, "my_table_2").withColumn("foo").withColumn("bar")
                        .withColumn("baz").execute();

                assertSame(table2.getSchema(), schema);
                assertSame(table2, schema.getTables()[1]);
                assertEquals(2, schema.getTableCount());

                cb.insertInto(table1).value("foo", 123.0).value("bar", "str 1").value("baz", true).execute();
            }
        });

        dc.refreshSchemas();

        Schema schema = dc.getDefaultSchema();
        assertEquals(2, schema.getTableCount());
        assertEquals("[my_table_1, my_table_2]", Arrays.toString(schema.getTableNames()));

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                cb.insertInto(dc.getTableByQualifiedLabel("my_table_1")).value("foo", 456.2)
                        .value("bar", "парфюмерия +и косметика").value("baz", false).execute();
            }
        });

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback cb) {
                cb.insertInto("my_table_1").value("foo", 789).value("bar", DateUtils.get(2011, Month.JULY, 8))
                        .value("baz", false).execute();
            }
        });

        DataSet ds = dc.query().from("my_table_1").select("foo").and("bar").and("baz").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[123, str 1, true]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[456.2, парфюмерия +и косметика, false]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[789, 2011-07-08 00:00:00, false]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.deleteFrom("my_table_1").where("foo").greaterThan("124").execute();
            }
        });

        assertEquals("1",
                MetaModelHelper.executeSingleRowQuery(dc, dc.query().from("my_table_1").selectCount().toQuery())
                        .getValue(0).toString());

        ds = dc.query().from("my_table_1").select("foo").and("bar").and("baz").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[123, str 1, true]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.dropTable("my_table_1").execute();
            }
        });

        assertEquals("[my_table_2]", Arrays.toString(schema.getTableNames()));

        dc.refreshSchemas();

        assertEquals("[my_table_2]", Arrays.toString(dc.getDefaultSchema().getTableNames()));
        assertEquals(1, dc.getDefaultSchema().getTableCount());
    }

    public void testGetStyles() throws Exception {
        DataContext dc = new ExcelDataContext(copyOf("src/test/resources/styles.xlsx"));
        Table table = dc.getDefaultSchema().getTables()[0];
        assertEquals("[style name, example]", Arrays.toString(table.getColumnNames()));

        DataSet ds = dc.query().from(table).select(table.getColumns()).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[bold, foo]]", ds.getRow().toString());
        assertEquals("", ds.getRow().getStyle(0).toCSS());
        assertEquals("font-weight: bold;", ds.getRow().getStyle(1).toCSS());

        assertTrue(ds.next());
        assertEquals("Row[values=[italic, foo]]", ds.getRow().toString());
        assertEquals("", ds.getRow().getStyle(0).toCSS());
        assertEquals("font-style: italic;", ds.getRow().getStyle(1).toCSS());

        assertTrue(ds.next());
        assertEquals("Row[values=[underline, foo]]", ds.getRow().toString());
        assertEquals("", ds.getRow().getStyle(0).toCSS());
        assertEquals("text-decoration: underline;", ds.getRow().getStyle(1).toCSS());

        assertTrue(ds.next());
        assertEquals("Row[values=[custom text col, foo]]", ds.getRow().toString());
        assertEquals("", ds.getRow().getStyle(0).toCSS());
        assertEquals("color: rgb(138,67,143);", ds.getRow().getStyle(1).toCSS());

        assertTrue(ds.next());
        assertEquals("Row[values=[yellow text col, foo]]", ds.getRow().toString());
        assertEquals("", ds.getRow().getStyle(0).toCSS());
        assertEquals("color: rgb(255,255,0);", ds.getRow().getStyle(1).toCSS());

        assertTrue(ds.next());
        assertEquals("Row[values=[custom bg, foo]]", ds.getRow().toString());
        assertEquals("", ds.getRow().getStyle(0).toCSS());
        assertEquals("background-color: rgb(136,228,171);", ds.getRow().getStyle(1).toCSS());

        assertTrue(ds.next());
        assertEquals("Row[values=[yellow bg, foo]]", ds.getRow().toString());
        assertEquals("", ds.getRow().getStyle(0).toCSS());
        assertEquals("background-color: rgb(255,255,0);", ds.getRow().getStyle(1).toCSS());

        assertTrue(ds.next());
        assertEquals("Row[values=[center align, foo]]", ds.getRow().toString());
        assertEquals("", ds.getRow().getStyle(0).toCSS());
        assertEquals("text-align: center;", ds.getRow().getStyle(1).toCSS());

        assertTrue(ds.next());
        assertEquals("Row[values=[font size 8, foo]]", ds.getRow().toString());
        assertEquals("", ds.getRow().getStyle(0).toCSS());
        assertEquals("font-size: 8pt;", ds.getRow().getStyle(1).toCSS());

        assertTrue(ds.next());
        assertEquals("Row[values=[font size 16, foo]]", ds.getRow().toString());
        assertEquals("", ds.getRow().getStyle(0).toCSS());
        assertEquals("font-size: 16pt;", ds.getRow().getStyle(1).toCSS());

        assertFalse(ds.next());
    }

    /**
     * Tests that you can execute a query on a ExcelDataContext even though the
     * schema has not yet been (explicitly) loaded.
     */
    public void testExecuteQueryBeforeLoadingSchema() throws Exception {
        // first use one DataContext to retreive the schema/table/column objects
        ExcelDataContext dc1 = new ExcelDataContext(copyOf("src/test/resources/Spreadsheet2007.xlsx"));
        Schema schema = dc1.getDefaultSchema();
        Table table = schema.getTable(0);
        Column column = table.getColumn(0);

        // query another DataContext using the schemas of the one above
        ExcelDataContext dc2 = new ExcelDataContext(copyOf("src/test/resources/Spreadsheet2007.xlsx"));
        DataSet ds = dc2.executeQuery(new Query().from(table).select(column));

        // previously we would crash at this point!

        assertNotNull(ds);
        ds.close();
    }

    public void testCustomColumnNames() throws Exception {
        final String firstColumnName = "first";
        final String secondColumnName = "second";
        final String thirdColumnName = "third";

        final ExcelConfiguration configuration = new ExcelConfiguration(ExcelConfiguration.DEFAULT_COLUMN_NAME_LINE,
                new CustomColumnNamingStrategy(firstColumnName, secondColumnName, thirdColumnName), true, false);
        final DataContext dataContext = new ExcelDataContext(copyOf("src/test/resources/Spreadsheet2007.xlsx"),
                configuration);
        final Table table = dataContext.getDefaultSchema().getTable(0);

        assertNotNull(table.getColumnByName(firstColumnName));
        assertNotNull(table.getColumnByName(secondColumnName));
        assertNotNull(table.getColumnByName(thirdColumnName));
    }
}