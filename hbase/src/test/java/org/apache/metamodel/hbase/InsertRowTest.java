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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.MutableTable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class InsertRowTest extends HBaseUpdateCallbackTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

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
     * Having the table type wrong, should throw an exception
     *
     * @throws IOException
     */
    @Test
    public void testWrongTableType() throws IOException {
        final MutableTable mutableTable = new MutableTable();
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Not an HBase table: " + mutableTable);

        getUpdateCallback().insertInto(mutableTable);
    }

    /**
     * Using a table that doesn't exist in the schema, should throw an exception
     *
     * @throws IOException
     */
    @Test
    public void testTableThatDoesntExist() throws IOException {
        final HBaseTable wrongTable = createHBaseTable("NewTableNotInSchema", HBaseDataContext.FIELD_ID, "cf1", "cf2");

        exception.expect(MetaModelException.class);
        exception.expectMessage("Trying to insert data into table: " + wrongTable.getName()
                + ", which doesn't exist yet");

        createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR);

        getUpdateCallback().insertInto(wrongTable);
    }

    /**
     * If the ID-column doesn't exist in the columns array, then a exception should be thrown
     *
     * @throws IOException
     */
    @Test
    public void testIDColumnDoesntExistInColumnsArray() throws IOException {
        exception.expect(MetaModelException.class);
        exception.expectMessage("The ID-Column was not found");

        final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                CF_BAR);

        final RowInsertionBuilder rowInsertionBuilder = getUpdateCallback().insertInto(existingTable);
        rowInsertionBuilder.execute();
    }

    /**
     * Inserting a row without setting enough values directly on the HBaseClient, should throw exception.
     * NOTE: This exception is already prevented when using the {@link HBaseRowInsertionBuilder}
     * @throws IOException
     */
    @Test
    public void testNotSettingEnoughValues() throws IOException {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("The amount of columns don't match the amount of values");

        final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                CF_BAR);
        final Map<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, false);
        final List<HBaseColumn> columns = getHBaseColumnsFromRow(row);
        final Collection<Object> values = getTooLittleValues(row);
        final HBaseClient hBaseClient = ((HBaseDataContext) getUpdateCallback().getDataContext()).getHBaseClient();
        hBaseClient.insertRow(TABLE_NAME, columns.toArray(new HBaseColumn[columns.size()]), values.toArray(
                new Object[values.size()]), 0); // TODO: find the ID-column
    }

    private Collection<Object> getTooLittleValues(final Map<HBaseColumn, Object> row) {
        Collection<Object> values = row.values();
        values.remove(V_123_BYTE_ARRAY);
        return values;
    }

    /**
     * Inserting a row with with a value null, should get skipped
     * @throws IOException 
     */
    @Test
    public void testInsertRowWithValueNull() throws IOException {
        final HBaseTable table = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR);

        RowInsertionBuilder insertBuilder = getUpdateCallback()
                .insertInto(table)
                .value(new HBaseColumn(HBaseDataContext.FIELD_ID, null, table), RK_1)
                .value(new HBaseColumn(CF_FOO, Q_BAH, table), V_WORLD)
                .value(new HBaseColumn(CF_FOO, Q_HELLO, table), null)
                .value(new HBaseColumn(CF_BAR, Q_HEY, table), V_YO);
        insertBuilder.execute();

        try (org.apache.hadoop.hbase.client.Table hBaseTable = getDataContext().getConnection().getTable(TableName
                .valueOf(TABLE_NAME))) {
            final Get get = new Get(Bytes.toBytes(RK_1));
            final Result result = hBaseTable.get(get);

            assertFalse(result.isEmpty());
            assertEquals(V_WORLD, new String(result.getValue(Bytes.toBytes(CF_FOO), Bytes.toBytes(Q_BAH))));
            assertNull(result.getValue(Bytes.toBytes(CF_FOO), Bytes.toBytes(Q_HELLO)));
            assertEquals(V_YO, new String(result.getValue(Bytes.toBytes(CF_BAR), Bytes.toBytes(Q_HEY))));
        }
    }

    /**
     * Goodflow. Using an existing table and columns, should work
     *
     * @throws IOException
     */
    @Test
    public void testInsertIntoWithoutExecute() throws IOException {
        final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                CF_BAR);
        getUpdateCallback().insertInto(existingTable);
    }

    /**
     * Goodflow, creating a row with qualifiers null should work.
     *
     * @throws IOException
     */
    @Test
    public void testQualifierNull() throws IOException {
        final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                CF_BAR);
        final Map<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, true);

        checkRows(false, true);
        final RowInsertionBuilder rowInsertionBuilder = getUpdateCallback().insertInto(existingTable);
        setValuesInInsertionBuilder(row, rowInsertionBuilder);
        rowInsertionBuilder.execute();
        checkRows(true, true);
    }

    /**
     * Goodflow. Inserting a row succesfully (with values set)
     *
     * @throws IOException
     */
    @Test
    public void testInsertingSuccesfully() throws IOException {
        final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                CF_BAR);
        final Map<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, false);

        checkRows(false, false);
        final RowInsertionBuilder rowInsertionBuilder = getUpdateCallback().insertInto(existingTable);
        setValuesInInsertionBuilder(row, rowInsertionBuilder);
        rowInsertionBuilder.execute();
        checkRows(true, false);
    }

    @Test
    public void testSqlRepresentation() throws IOException {
        final HBaseTable table = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR);

        RowInsertionBuilder insertBuilder = getUpdateCallback()
                .insertInto(table)
                .value(new HBaseColumn(HBaseDataContext.FIELD_ID, null, table), RK_1)
                .value(new HBaseColumn(CF_FOO, Q_BAH, table), V_WORLD)
                .value(new HBaseColumn(CF_FOO, Q_HELLO, table), V_THERE)
                .value(new HBaseColumn(CF_BAR, Q_HEY, table), V_YO);

        assertEquals("INSERT INTO HBase.table_for_junit(_id,foo:bah,foo:hello,bar:hey) "
                + "VALUES (\"junit1\",\"world\",\"there\",\"yo\")", insertBuilder.toSql());
    }
}
