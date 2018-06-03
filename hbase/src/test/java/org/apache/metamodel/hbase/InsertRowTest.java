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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.schema.MutableTable;
import org.junit.Test;

public class InsertRowTest extends HBaseUpdateCallbackTest {

    /**
     * Check if inserting into a table is supported
     *
     * @throws IOException
     */
    @Test
    public void testInsertSupported() throws IOException {
        assertTrue(getUpdateCallback().isInsertSupported());
    }

    /**
     * Using only the table parameter, should throw an exception
     *
     * @throws IOException
     */
    @Test
    public void testOnlyUsingTableParameter() throws IOException {
        try {
            final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR);
            getUpdateCallback().insertInto(existingTable);
            fail("Should get an exception that this method is not supported");
        } catch (UnsupportedOperationException e) {
            assertEquals("We need an explicit list of columns when inserting into an HBase table.", e.getMessage());
        }
    }

    /**
     * Having the table type wrong, should throw an exception
     *
     * @throws IOException
     */
    @Test
    public void testWrongTableType() throws IOException {
        final MutableTable mutableTable = new MutableTable();
        try {
            final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR);
            final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR, false);
            final List<HBaseColumn> columns = getHBaseColumnsFromRow(row);
            getUpdateCallback().insertInto(mutableTable, columns);
            fail("Should get an exception that the type of the table is wrong.");
        } catch (IllegalArgumentException e) {
            assertEquals("Not an HBase table: " + mutableTable, e.getMessage());
        }
    }

    /**
     * Having the columns parameter null at the updateCallBack, should throw an exception
     *
     * @throws IOException
     */
    @Test
    public void testColumnsNullAtUpdateCallBack() throws IOException {
        try {
            final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR);
            getUpdateCallback().insertInto(existingTable, null);
            fail("Should get an exception that the columns list is null.");
        } catch (IllegalArgumentException e) {
            assertEquals("The hbaseColumns list is null or empty", e.getMessage());
        }
    }

    /**
     * Having the columns parameter empty at the updateCallBack, should throw an exception
     *
     * @throws IOException
     */
    @Test
    public void testColumnsEmptyAtUpdateCallBack() throws IOException {
        try {
            final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR);
            getUpdateCallback().insertInto(existingTable, new ArrayList<HBaseColumn>());
            fail("Should get an exception that the columns list is empty.");
        } catch (IllegalArgumentException e) {
            assertEquals("The hbaseColumns list is null or empty", e.getMessage());
        }
    }

    /**
     * Using a table that doesn't exist in the schema, should throw an exception
     *
     * @throws IOException
     */
    @Test
    public void testTableThatDoesntExist() throws IOException {
        final HBaseTable wrongTable = createHBaseTable("NewTableNotInSchema", HBaseDataContext.FIELD_ID, "cf1", "cf2",
                null);
        try {
            final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR);
            final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR, false);
            final List<HBaseColumn> columns = getHBaseColumnsFromRow(row);
            getUpdateCallback().insertInto(wrongTable, columns);
            fail("Should get an exception that the table isn't in the schema.");
        } catch (MetaModelException e) {
            assertEquals("Trying to insert data into table: " + wrongTable.getName() + ", which doesn't exist yet", e
                    .getMessage());
        }
    }

    /**
     * If the ID-column doesn't exist in the columns array, then a exception should be thrown
     *
     * @throws IOException
     */
    @Test
    public void testIDColumnDoesntExistInColumnsArray() throws IOException {
        try {
            final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR);
            final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, null, CF_FOO, CF_BAR, false);
            final List<HBaseColumn> columns = getHBaseColumnsFromRow(row);
            getUpdateCallback().insertInto(existingTable, columns);
            fail("Should get an exception that ID-column doesn't exist.");
        } catch (MetaModelException e) {
            assertEquals("The ID-Column was not found", e.getMessage());
        }
    }

    /**
     * If the column family doesn't exist in the table (wrong columnFamily), then a exception should be thrown
     *
     * @throws IOException
     */
    @Test
    public void testColumnFamilyDoesntExistsBecauseItsNull() throws IOException {
        final String wrongColumnFamily = "wrongColumnFamily";
        try {
            final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR);
            final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR, false);
            final List<HBaseColumn> columns = getHBaseColumnsFromRow(row);
            final HBaseTable wrongTable = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                    wrongColumnFamily, null);
            getUpdateCallback().insertInto(wrongTable, columns);
            fail("Should get an exception that the columnFamily doesn't exist.");
        } catch (MetaModelException e) {
            assertEquals(String.format("ColumnFamily: %s doesn't exist in the schema of the table", wrongColumnFamily),
                    e.getMessage());
        }
    }

    /**
     * If the column family doesn't exist in the table (new columnFamily), then a exception should be thrown
     *
     * @throws IOException
     */
    @Test
    public void testColumnFamilyDoesntExistsBecauseItsNew() throws IOException {
        final String wrongColumnFamily = "newColumnFamily";
        try {
            final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR);
            final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR, false);
            final List<HBaseColumn> columns = getHBaseColumnsFromRow(row);
            final HBaseTable wrongTable = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR,
                    wrongColumnFamily);
            getUpdateCallback().insertInto(wrongTable, columns);
            fail("Should get an exception that the columnFamily doesn't exist.");
        } catch (MetaModelException e) {
            assertEquals(String.format("ColumnFamily: %s doesn't exist in the schema of the table", wrongColumnFamily),
                    e.getMessage());
        }
    }

    /**
     * Creating a HBaseClient with the tableName null, should throw a exception
     */
    @Test
    public void testCreatingTheHBaseClientWithTableNameNull() {
        try {
            final HBaseTable table = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, null);
            final LinkedHashMap<HBaseColumn, Object> row = createRow(table, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR,
                    false);
            final HBaseColumn[] columns = convertToHBaseColumnsArray(getHBaseColumnsFromRow(row));
            final Object[] values = new String[] { "Values" };
            new HBaseClient(getDataContext().getConnection()).insertRow(null, columns, values, 0);
            fail("Should get an exception that tableName is null");
        } catch (IllegalArgumentException e) {
            assertEquals("Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn", e
                    .getMessage());
        }
    }

    /**
     * Creating a HBaseClient with the columns null, should throw a exception
     */
    @Test
    public void testCreatingTheHBaseClientWithColumnsNull() {
        try {
            final Object[] values = new String[] { "Values" };
            new HBaseClient(getDataContext().getConnection()).insertRow("tableName", null, values, 0);
            fail("Should get an exception that columns is null");
        } catch (IllegalArgumentException e) {
            assertEquals("Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn", e
                    .getMessage());
        }
    }

    /**
     * Creating a HBaseClient with the values null, should throw a exception
     */
    @Test
    public void testCreatingTheHBaseClientWithValuesNull() {
        try {
            final HBaseTable table = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, null);
            final LinkedHashMap<HBaseColumn, Object> row = createRow(table, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR,
                    false);
            final HBaseColumn[] columns = convertToHBaseColumnsArray(getHBaseColumnsFromRow(row));
            new HBaseClient(getDataContext().getConnection()).insertRow(table.getName(), columns, null, 0);
            fail("Should get an exception that values is null");
        } catch (IllegalArgumentException e) {
            assertEquals("Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn", e
                    .getMessage());
        }
    }

    /**
     * Creating a HBaseClient with the indexOfIdColumn out of bounce, should throw a exception
     */
    @Test
    public void testCreatingTheHBaseClientWithIndexOfIdColumnOutOfBounce() {
        try {
            final HBaseTable table = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, null);
            final LinkedHashMap<HBaseColumn, Object> row = createRow(table, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR,
                    false);
            final HBaseColumn[] columns = convertToHBaseColumnsArray(getHBaseColumnsFromRow(row));
            final Object[] values = new String[] { "Values" };
            new HBaseClient(getDataContext().getConnection()).insertRow(table.getName(), columns, values, 10);
            fail("Should get an exception that the indexOfIdColumn is incorrect");
        } catch (IllegalArgumentException e) {
            assertEquals("Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn", e
                    .getMessage());
        }
    }

    /**
     * Creating a HBaseClient with the rowKey null, should throw a exception
     */
    @Test
    public void testCreatingTheHBaseClientWithRowKeyNull() {
        try {
            final HBaseTable table = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, null);
            final LinkedHashMap<HBaseColumn, Object> row = createRow(table, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR,
                    false);
            final HBaseColumn[] columns = convertToHBaseColumnsArray(getHBaseColumnsFromRow(row));
            final Object[] values = new String[] { null };
            new HBaseClient(getDataContext().getConnection()).insertRow(table.getName(), columns, values, 0);
            fail("Should get an exception that the indexOfIdColumn is incorrect");
        } catch (IllegalArgumentException e) {
            assertEquals("Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn", e
                    .getMessage());
        }
    }

    /**
     * Inserting a row without setting enough values directly on the HBaseClient, should throw exception.
     * NOTE: This exception is already prevented when using the {@link HBaseRowInsertionBuilder}
     * @throws IOException 
     */
    @Test
    public void testNotSettingEnoughValues() throws IOException {
        try {
            final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR);
            final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR, false);
            final List<HBaseColumn> columns = getHBaseColumnsFromRow(row);
            final Collection<Object> values = getToLittleValues(row);
            final HBaseClient hBaseClient = ((HBaseDataContext) getUpdateCallback().getDataContext()).getHBaseClient();
            hBaseClient.insertRow(TABLE_NAME, columns.toArray(new HBaseColumn[columns.size()]), values.toArray(
                    new Object[values.size()]), 0); // TODO: find the ID-column
            fail("Should get an exception when insering directly into the HBaseClient without having enough values.");
        } catch (IllegalArgumentException e) {
            assertEquals("The amount of columns don't match the amount of values", e.getMessage());
        }
    }

    /**
     * Goodflow. Using an existing table and columns, should work
     */
    @Test
    public void testInsertIntoWithoutExecute() {
        try {
            final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR);
            final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR, false);
            final List<HBaseColumn> columns = getHBaseColumnsFromRow(row);
            getUpdateCallback().insertInto(existingTable, columns);
        } catch (Exception e) {
            fail("No exception should be thrown, when inserting into an existing table.");
        }
    }

    /**
     * Goodflow, creating a row with qualifiers null should work.
     */
    @Test
    public void testQaulifierNull() {
        try {
            final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR);
            final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR, true);
            final List<HBaseColumn> columns = getHBaseColumnsFromRow(row);

            checkRows(false, true);
            final HBaseRowInsertionBuilder rowInsertionBuilder = getUpdateCallback().insertInto(existingTable, columns);
            setValuesInInsertionBuilder(row, rowInsertionBuilder);
            rowInsertionBuilder.execute();
            checkRows(true, true);
        } catch (Exception e) {
            fail("Inserting a row without qualifiers should work.");
        }
    }

    /**
     * Goodflow. Inserting a row succesfully (with values set)
     */
    @Test
    public void testInsertingSuccesfully() {
        try {
            final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR);
            final LinkedHashMap<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID, CF_FOO,
                    CF_BAR, false);
            final List<HBaseColumn> columns = getHBaseColumnsFromRow(row);

            checkRows(false, false);
            final HBaseRowInsertionBuilder rowInsertionBuilder = getUpdateCallback().insertInto(existingTable, columns);
            setValuesInInsertionBuilder(row, rowInsertionBuilder);
            rowInsertionBuilder.execute();
            checkRows(true, false);
        } catch (Exception e) {
            fail("No exception should be thrown, when inserting with values.");
        }
    }

    /**
     * Converts a list of {@link HBaseColumn}'s to an array of {@link HBaseColumn}'s
     *
     * @param columns
     * @return Array of {@link HBaseColumn}
     */
    private static HBaseColumn[] convertToHBaseColumnsArray(List<HBaseColumn> columns) {
        return columns.toArray(new HBaseColumn[columns.size()]);
    }
}
