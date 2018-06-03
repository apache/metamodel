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
import java.util.Arrays;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;
import org.junit.Before;
import org.junit.Test;

public class HBaseDataContextTest extends HBaseTestCase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        createTableNatively();
    }

    @Test
    public void testCreateInsertQueryAndDrop() throws Exception {
        // test the schema exploration
        final Table table = getDataContext().getDefaultSchema().getTableByName(TABLE_NAME);
        assertNotNull(table);

        assertEquals("[" + HBaseDataContext.FIELD_ID + ", " + CF_BAR + ", " + CF_FOO + "]", Arrays.toString(table
                .getColumnNames()
                .toArray()));
        assertEquals(HBaseColumn.DEFAULT_COLUMN_TYPE_FOR_COLUMN_FAMILIES, table.getColumn(1).getType());

        // insert two records
        insertRecordsNatively();

        // query using regular configuration
        try (final DataSet dataSet1 = getDataContext().query().from(TABLE_NAME).selectAll().execute()) {
            assertTrue(dataSet1.next());
            assertEquals("Row[values=[" + RK_1 + ", {" + Q_HEY + "=" + V_YO + "," + Q_HI + "=" + V_THERE + "}, {"
                    + Q_HELLO + "=" + V_WORLD + "}]]", dataSet1.getRow().toString());
            assertTrue(dataSet1.next());
            assertEquals("Row[values=[" + RK_2 + ", {" + Q_BAH + "=" + new String(V_123_BYTE_ARRAY) + "," + Q_HI + "="
                    + V_YOU + "}, {}]]", dataSet1.getRow().toString());
            assertFalse(dataSet1.next());
        }

        // query using custom table definitions
        final String columnName1 = CF_FOO;
        final String columnName2 = CF_BAR + ":" + Q_HI;
        final String columnName3 = CF_BAR + ":" + Q_HEY;
        final String[] columnNames = new String[] { columnName1, columnName2, columnName3 };
        final ColumnType[] columnTypes = new ColumnType[] { ColumnType.MAP, ColumnType.VARCHAR, ColumnType.VARCHAR };
        final SimpleTableDef[] tableDefinitions = new SimpleTableDef[] { new SimpleTableDef(TABLE_NAME, columnNames,
                columnTypes) };
        setDataContext(new HBaseDataContext(new HBaseConfiguration("SCH", getZookeeperHostname(), getZookeeperPort(),
                tableDefinitions, ColumnType.VARCHAR)));

        try (final DataSet dataSet2 = getDataContext()
                .query()
                .from(TABLE_NAME)
                .select(columnName1, columnName2, columnName3)
                .execute()) {
            assertTrue(dataSet2.next());
            assertEquals("Row[values=[{" + Q_HELLO + "=" + V_WORLD + "}, " + V_THERE + ", " + V_YO + "]]", dataSet2
                    .getRow()
                    .toString());
            assertTrue(dataSet2.next());
            assertEquals("Row[values=[{}, " + V_YOU + ", null]]", dataSet2.getRow().toString());
            assertFalse(dataSet2.next());
        }

        // query count
        try (final DataSet dataSet3 = getDataContext().query().from(TABLE_NAME).selectCount().execute()) {
            assertTrue(dataSet3.next());
            assertEquals("Row[values=[" + NUMBER_OF_ROWS + "]]", dataSet3.getRow().toString());
            assertFalse(dataSet3.next());
        }

        // query only id
        try (final DataSet dataSet4 = getDataContext()
                .query()
                .from(TABLE_NAME)
                .select(HBaseDataContext.FIELD_ID)
                .execute()) {
            assertTrue(dataSet4.next());
            assertEquals("Row[values=[" + RK_1 + "]]", dataSet4.getRow().toString());
            assertTrue(dataSet4.next());
            assertEquals("Row[values=[" + RK_2 + "]]", dataSet4.getRow().toString());
            assertFalse(dataSet4.next());
        }

        // primary key lookup query - using GET
        try (final DataSet dataSet5 = getDataContext()
                .query()
                .from(TABLE_NAME)
                .select(HBaseDataContext.FIELD_ID)
                .where(HBaseDataContext.FIELD_ID)
                .eq(RK_1)
                .execute()) {
            assertTrue(dataSet5.next());
            assertEquals("Row[values=[" + RK_1 + "]]", dataSet5.getRow().toString());
            assertFalse(dataSet5.next());
        }
    }

    private void insertRecordsNatively() throws IOException, InterruptedException {
        try (final org.apache.hadoop.hbase.client.Table hTable = getDataContext().getHTable(TABLE_NAME)) {
            final Put put1 = new Put(RK_1.getBytes());
            put1.addColumn(CF_FOO.getBytes(), Q_HELLO.getBytes(), V_WORLD.getBytes());
            put1.addColumn(CF_BAR.getBytes(), Q_HI.getBytes(), V_THERE.getBytes());
            put1.addColumn(CF_BAR.getBytes(), Q_HEY.getBytes(), V_YO.getBytes());

            final Put put2 = new Put(RK_2.getBytes());
            put2.addColumn(CF_BAR.getBytes(), Q_BAH.getBytes(), V_123_BYTE_ARRAY);
            put2.addColumn(CF_BAR.getBytes(), Q_HI.getBytes(), V_YOU.getBytes());

            final Object[] result = new Object[NUMBER_OF_ROWS];
            hTable.batch(Arrays.asList(put1, put2), result);
        }
    }

    private void createTableNatively() throws IOException {
        try (Admin admin = getDataContext().getAdmin()) {
            final TableName tableName = TableName.valueOf(TABLE_NAME);

            // Check if the table exists
            if (admin.isTableAvailable(tableName)) {
                // table already exists
                System.out.println("Unittest table already exists: " + TABLE_NAME);
            } else {
                // Create table
                System.out.println("Creating table");
                final HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                tableDescriptor.addFamily(new HColumnDescriptor(CF_FOO.getBytes()));
                tableDescriptor.addFamily(new HColumnDescriptor(CF_BAR.getBytes()));
                admin.createTable(tableDescriptor);
                System.out.println("Created table");
            }
        }
    }
}
