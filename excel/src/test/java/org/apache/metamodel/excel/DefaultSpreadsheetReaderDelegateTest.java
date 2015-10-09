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
import java.lang.reflect.Field;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.FileHelper;

import junit.framework.TestCase;

public class DefaultSpreadsheetReaderDelegateTest extends TestCase {

    /**
     * Creates a copy of a particular file - to avoid changing of Excel files
     * under source control
     * 
     * @param path
     * @return
     */
    private File copyOf(String path) {
        final File srcFile = new File(path);
        return copyOf(srcFile);
    }

    private File copyOf(File srcFile) {
        final File destFile = new File("target/" + getName() + "-" + srcFile.getName());
        FileHelper.copy(srcFile, destFile);
        return destFile;
    }

    public void testReadAllTestResourceFiles() {
        File[] listFiles = new File("src/test/resources").listFiles();
        for (File file : listFiles) {
            if (file.isFile() && file.getName().indexOf(".xls") != -1) {
                try {
                    runTest(file);
                } catch (Throwable e) {
                    throw new IllegalStateException("Exception in file: " + file, e);
                }
            }
        }
    }

    private void runTest(File file) throws Exception {
        file = copyOf(file);
        ExcelDataContext mainDataContext = new ExcelDataContext(file);
        applyReaderDelegate(mainDataContext);

        ExcelDataContext comparedDataContext = null;
        if (file.getName().endsWith(".xlsx")) {
            comparedDataContext = new ExcelDataContext(file);
        }

        Schema schema = mainDataContext.getDefaultSchema();
        assertNotNull(schema);
        assertEquals(file.getName(), schema.getName());

        if (comparedDataContext != null) {
            assertEquals(comparedDataContext.getDefaultSchema().getName(), schema.getName());
        }

        assertEquals(DefaultSpreadsheetReaderDelegate.class, mainDataContext.getSpreadsheetReaderDelegateClass());

        Table[] tables = schema.getTables();
        assertTrue(tables.length > 0);

        Table[] comparedTables = null;
        if (comparedDataContext != null) {
            assertEquals(XlsxSpreadsheetReaderDelegate.class, comparedDataContext.getSpreadsheetReaderDelegateClass());
            comparedTables = comparedDataContext.getDefaultSchema().getTables();
            assertEquals(comparedTables.length, tables.length);
        }

        for (int i = 0; i < tables.length; i++) {
            Table table = tables[i];
            Column[] columns = table.getColumns();
            Query query = mainDataContext.query().from(table).select(columns).toQuery();
            DataSet dataSet = mainDataContext.executeQuery(query);

            DataSet comparedDataSet = null;
            if (comparedDataContext != null) {
                Table comparedTable = comparedTables[i];
                assertEquals(comparedTable.getName(), table.getName());
                assertEquals(comparedTable.getColumnCount(), table.getColumnCount());

                Column[] comparedColumns = comparedTable.getColumns();
                for (int j = 0; j < comparedColumns.length; j++) {
                    assertEquals(columns[j].getColumnNumber(), comparedColumns[j].getColumnNumber());
                }

                Query comparedQuery = comparedDataContext.query().from(comparedTable).select(comparedColumns).toQuery();
                comparedDataSet = comparedDataContext.executeQuery(comparedQuery);
            }

            while (dataSet.next()) {
                Row row = dataSet.getRow();
                assertNotNull(row);
                Object[] values = row.getValues();

                assertEquals(values.length, table.getColumnCount());

                if (comparedDataSet != null) {
                    boolean next = comparedDataSet.next();
                    assertTrue("No comparable row exists for: " + row, next);
                    Row comparedRow = comparedDataSet.getRow();
                    assertNotNull(comparedRow);
                    Object[] comparedValues = comparedRow.getValues();
                    assertEquals(comparedValues.length, table.getColumnCount());

                    for (int j = 0; j < comparedValues.length; j++) {
                        assertEquals(comparedValues[j], values[j]);
                    }

                    // compare styles
                    for (int j = 0; j < comparedValues.length; j++) {
                        Style style1 = comparedRow.getStyle(j);
                        Style style2 = row.getStyle(j);
                        assertEquals("Diff in style on row: " + row + " (value index = " + j + ")\nStyle 1: " + style1
                                + "\nStyle 2: " + style2 + ". ", style1, style2);
                    }
                }
            }
            dataSet.close();

            if (comparedDataSet != null) {
                assertFalse(comparedDataSet.next());
                comparedDataSet.close();
            }
        }
    }

    /**
     * Applies the {@link DefaultSpreadsheetReaderDelegate} through reflection.
     * 
     * @param dataContext
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    private void applyReaderDelegate(ExcelDataContext dataContext) throws NoSuchFieldException, IllegalAccessException {
        final SpreadsheetReaderDelegate delegate = new DefaultSpreadsheetReaderDelegate(dataContext.getResource(),
                dataContext.getConfiguration());
        final Field field = ExcelDataContext.class.getDeclaredField("_spreadsheetReaderDelegate");
        assertNotNull(field);
        field.setAccessible(true);
        field.set(dataContext, delegate);
    }

    public void testStylingOfDateCell() throws Exception {
        ExcelDataContext dc = new ExcelDataContext(copyOf("src/test/resources/Spreadsheet2007.xlsx"));
        applyReaderDelegate(dc);

        Table table = dc.getDefaultSchema().getTables()[0];

        final String expectedStyling = "";

        DataSet dataSet = dc.query().from(table).select("date").execute();
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertFalse(dataSet.next());
        dataSet.close();
    }

    public void testStylingOfNullCell() throws Exception {
        ExcelDataContext dc = new ExcelDataContext(copyOf("src/test/resources/formulas.xlsx"));
        applyReaderDelegate(dc);

        Table table = dc.getDefaultSchema().getTables()[0];

        DataSet dataSet = dc.query().from(table).select("Foo").and("Bar").where("Foo").isEquals("7").execute();
        assertTrue(dataSet.next());
        Row row = dataSet.getRow();
        assertNotNull(row.getStyle(0));

        final String expectedStyling = "";

        assertEquals(expectedStyling, row.getStyle(0).toCSS());
        assertNotNull(row.getStyle(1));
        assertEquals(expectedStyling, row.getStyle(1).toCSS());
        assertFalse(dataSet.next());
        dataSet.close();

        dataSet = dc.query().from(table).select("Foo").and("Bar").execute();
        assertTrue(dataSet.next());
        row = dataSet.getRow();
        assertNotNull(row.getStyle(0));
        assertEquals(expectedStyling, row.getStyle(0).toCSS());
        assertNotNull(row.getStyle(1));
        assertEquals(expectedStyling, row.getStyle(1).toCSS());

        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertTrue(dataSet.next());
        assertEquals(expectedStyling, dataSet.getRow().getStyle(0).toCSS());
        assertFalse(dataSet.next());
        dataSet.close();
    }
}
