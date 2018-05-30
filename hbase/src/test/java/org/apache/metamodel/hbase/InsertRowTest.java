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
package org.apache.metamodel.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableTable;

public class InsertRowTest extends HBaseUpdateCallbackTest {

    /**
     * Check if inserting into a table is supported
     * @throws IOException
     */
    public void testInsertSupported() throws IOException {
        if (isConfigured()) {
            assertTrue(getUpdateCallback().isInsertSupported());
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * Using only the table parameter, should throw an exception
     * @throws IOException
     */
    public void testOnlyUsingTableParameter() throws IOException {
        if (isConfigured()) {
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                getUpdateCallback().insertInto(existingTable);
                fail("Should get an exception that this method is not supported");
            } catch (UnsupportedOperationException e) {
                assertEquals("We need an explicit list of columns when inserting into an HBase table.", e.getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * Having the table type wrong, should throw an exception
     * @throws IOException
     */
    public void testWrongTableType() throws IOException {
        if (isConfigured()) {
            final MutableTable mutableTable = new MutableTable();
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID,
                        CF_FOO, CF_BAR);
                final List<HBaseColumn> columns = getHBaseColumnsFromMap(row);
                getUpdateCallback().insertInto(mutableTable, columns);
                fail("Should get an exception that the type of the table is wrong.");
            } catch (IllegalArgumentException e) {
                assertEquals("Not an HBase table: " + mutableTable, e.getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * Having the columns parameter null at the updateCallBack, should throw an exception
     * @throws IOException
     */
    public void testColumnsNullAtUpdateCallBack() throws IOException {
        if (isConfigured()) {
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                getUpdateCallback().insertInto(existingTable, null);
                fail("Should get an exception that the columns list is null.");
            } catch (IllegalArgumentException e) {
                assertEquals("The hbaseColumns list is null or empty", e.getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * Having the columns parameter empty at the updateCallBack, should throw an exception
     * @throws IOException
     */
    public void testColumnsEmptyAtUpdateCallBack() throws IOException {
        if (isConfigured()) {
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                getUpdateCallback().insertInto(existingTable, new ArrayList<HBaseColumn>());
                fail("Should get an exception that the columns list is empty.");
            } catch (IllegalArgumentException e) {
                assertEquals("The hbaseColumns list is null or empty", e.getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * Having the columns parameter empty at the builder, should throw an exception
     * @throws IOException
     */
    public void testColumnsEmptyAtBuilder() throws IOException {
        if (isConfigured()) {
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                List<Column> emptyList = new ArrayList<>();
                new HBaseRowInsertionBuilder(getUpdateCallback(), existingTable, emptyList);
                fail("Should get an exception that the columns list is empty.");
            } catch (IllegalArgumentException e) {
                assertEquals("The hbaseColumns list is null or empty", e.getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * Using a table that doesn't exist in the schema, should throw an exception
     * @throws IOException
     */
    public void testTableThatDoesntExist() throws IOException {
        if (isConfigured()) {
            final HBaseTable wrongTable = createHBaseTable("NewTableNotInSchema", HBaseDataContext.FIELD_ID, "cf1",
                    "cf2", null);
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID,
                        CF_FOO, CF_BAR);
                final List<HBaseColumn> columns = getHBaseColumnsFromMap(row);
                getUpdateCallback().insertInto(wrongTable, columns);
                fail("Should get an exception that the table isn't in the schema.");
            } catch (MetaModelException e) {
                assertEquals("Trying to insert data into table: " + wrongTable.getName() + ", which doesn't exist yet",
                        e.getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * If the ID-column doesn't exist in the columns array, then a exception should be thrown
     * @throws IOException
     */
    public void testIDColumnDoesntExistInColumnsArray() throws IOException {
        if (isConfigured()) {
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, null, CF_FOO, CF_BAR);
                final List<HBaseColumn> columns = getHBaseColumnsFromMap(row);
                getUpdateCallback().insertInto(existingTable, columns);
                fail("Should get an exception that ID-column doesn't exist.");
            } catch (MetaModelException e) {
                assertEquals("The ID-Column was not found", e.getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * If the column family doesn't exist in the table (wrong columnFamily), then a exception should be thrown
     * @throws IOException
     */
    public void testColumnFamilyDoesntExistsBecauseItsNull() throws IOException {
        if (isConfigured()) {
            final String wrongColumnFamily = "wrongColumnFamily";
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID,
                        CF_FOO, CF_BAR);
                final List<HBaseColumn> columns = getHBaseColumnsFromMap(row);
                final HBaseTable wrongTable = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        wrongColumnFamily, null);
                getUpdateCallback().insertInto(wrongTable, columns);
                fail("Should get an exception that the columnFamily doesn't exist.");
            } catch (MetaModelException e) {
                assertEquals(String.format("ColumnFamily: %s doesn't exist in the schema of the table",
                        wrongColumnFamily), e.getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * If the column family doesn't exist in the table (new columnFamily), then a exception should be thrown
     * @throws IOException
     */
    public void testColumnFamilyDoesntExistsBecauseItsNew() throws IOException {
        if (isConfigured()) {
            final String wrongColumnFamily = "newColumnFamily";
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID,
                        CF_FOO, CF_BAR);
                final List<HBaseColumn> columns = getHBaseColumnsFromMap(row);
                final HBaseTable wrongTable = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR,
                        wrongColumnFamily);
                getUpdateCallback().insertInto(wrongTable, columns);
                fail("Should get an exception that the columnFamily doesn't exist.");
            } catch (MetaModelException e) {
                assertEquals(String.format("ColumnFamily: %s doesn't exist in the schema of the table",
                        wrongColumnFamily), e.getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * Creating a HBaseClient with the tableName null, should throw a exception
     */
    public void testCreatingTheHBaseClientWithTableNameNull() {
        if (isConfigured()) {
            try {
                final HBaseTable table = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, null);
                final LinkedHashMap<HBaseColumn, Object> row = createRow(table, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                final HBaseColumn[] columns = HBaseColumn.convertToHBaseColumnsArray(getHBaseColumnsFromMap(row));
                final Object[] values = new String[] { "Values" };
                new HBaseClient(getDataContext().getConnection()).insertRow(null, columns, values, 0);
                fail("Should get an exception that tableName is null");
            } catch (IllegalArgumentException e) {
                assertEquals(
                        "Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn", e
                                .getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * Creating a HBaseClient with the columns null, should throw a exception
     */
    public void testCreatingTheHBaseClientWithColumnsNull() {
        if (isConfigured()) {
            try {
                final Object[] values = new String[] { "Values" };
                new HBaseClient(getDataContext().getConnection()).insertRow("tableName", null, values, 0);
                fail("Should get an exception that columns is null");
            } catch (IllegalArgumentException e) {
                assertEquals(
                        "Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn", e
                                .getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * Creating a HBaseClient with the values null, should throw a exception
     */
    public void testCreatingTheHBaseClientWithValuesNull() {
        if (isConfigured()) {
            try {
                final HBaseTable table = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, null);
                final LinkedHashMap<HBaseColumn, Object> row = createRow(table, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                final HBaseColumn[] columns = HBaseColumn.convertToHBaseColumnsArray(getHBaseColumnsFromMap(row));
                new HBaseClient(getDataContext().getConnection()).insertRow(table.getName(), columns, null, 0);
                fail("Should get an exception that values is null");
            } catch (IllegalArgumentException e) {
                assertEquals(
                        "Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn", e
                                .getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * Creating a HBaseClient with the indexOfIdColumn out of bounce, should throw a exception
     */
    public void testCreatingTheHBaseClientWithIndexOfIdColumnOutOfBounce() {
        if (isConfigured()) {
            try {
                final HBaseTable table = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, null);
                final LinkedHashMap<HBaseColumn, Object> row = createRow(table, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                final HBaseColumn[] columns = HBaseColumn.convertToHBaseColumnsArray(getHBaseColumnsFromMap(row));
                final Object[] values = new String[] { "Values" };
                new HBaseClient(getDataContext().getConnection()).insertRow(table.getName(), columns, values, 10);
                fail("Should get an exception that the indexOfIdColumn is incorrect");
            } catch (IllegalArgumentException e) {
                assertEquals(
                        "Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn", e
                                .getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * Creating a HBaseClient with the rowKey null, should throw a exception
     */
    public void testCreatingTheHBaseClientWithRowKeyNull() {
        if (isConfigured()) {
            try {
                final HBaseTable table = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, null);
                final LinkedHashMap<HBaseColumn, Object> row = createRow(table, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                final HBaseColumn[] columns = HBaseColumn.convertToHBaseColumnsArray(getHBaseColumnsFromMap(row));
                final Object[] values = new String[] { null };
                new HBaseClient(getDataContext().getConnection()).insertRow(table.getName(), columns, values, 0);
                fail("Should get an exception that the indexOfIdColumn is incorrect");
            } catch (IllegalArgumentException e) {
                assertEquals(
                        "Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn", e
                                .getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * Goodflow. Using an existing table and columns, should work
     */
    public void testInsertIntoWithoutExecute() {
        if (isConfigured()) {
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID,
                        CF_FOO, CF_BAR);
                final List<HBaseColumn> columns = getHBaseColumnsFromMap(row);
                getUpdateCallback().insertInto(existingTable, columns);
            } catch (Exception e) {
                fail("No exception should be thrown, when inserting into an existing table.");
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    // /**
    // * Goodflow. Executing the insertInto with the Values being null, should not throw an exception
    // */
    // public void testNotSettingTheValues() {
    // if (isConfigured()) {
    // try {
    // final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
    // CF_BAR);
    // final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID,
    // CF_FOO, CF_BAR);
    // final List<HBaseColumn> columns = getHBaseColumnsFromMap(row);
    //
    // checkRows(false);
    // final HBaseRowInsertionBuilder rowInsertionBuilder = getUpdateCallback().insertInto(existingTable,
    // columns);
    // rowInsertionBuilder.execute();
    // checkRows(false);
    // } catch (Exception e) {
    // fail("No exception should be thrown, when inserting without values.");
    // }
    // } else {
    // warnAboutANotExecutedTest(getClass().getName(), new Object() {
    // }.getClass().getEnclosingMethod().getName());
    // }
    // }

    /**
     * Goodflow. Inserting a row succesfully (with values set)
     */
    public void testInsertingSuccesfully() {
        if (isConfigured()) {
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID,
                        CF_FOO, CF_BAR);
                final List<HBaseColumn> columns = getHBaseColumnsFromMap(row);

                checkRows(false);
                final HBaseRowInsertionBuilder rowInsertionBuilder = getUpdateCallback().insertInto(existingTable,
                        columns);
                setValuesInInsertionBuilder(row, rowInsertionBuilder);
                rowInsertionBuilder.execute();
                checkRows(true);
            } catch (Exception e) {
                fail("No exception should be thrown, when inserting with values.");
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }
}
