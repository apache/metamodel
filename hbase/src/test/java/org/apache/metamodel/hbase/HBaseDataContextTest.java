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

public class HBaseDataContextTest extends HBaseTestCase {

    // Table
    private static final String EXAMPLE_TABLE_NAME = "table_for_junit";

    // ColumnFamilies
    private static final String CF_FOO = "foo";
    private static final String CF_BAR = "bar";

    // Qualifiers
    private static final String Q_HELLO = "hello";
    private static final String Q_HI = "hi";
    private static final String Q_HEY = "hey";
    private static final String Q_BAH = "bah";

    // RowKeys
    private static final String RK_1 = "junit1";
    private static final String RK_2 = "junit2";

    private static final int NUMBER_OF_ROWS = 2;

    // Values
    private static final String V_WORLD = "world";
    private static final String V_THERE = "there";
    private static final String V_YO = "yo";
    private static final byte[] V_123_BYTE_ARRAY = new byte[] { 1, 2, 3 };
    private static final String V_YOU = "you";

    private HBaseDataContext _dataContext;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        if (isConfigured()) {
            final String zookeeperHostname = getZookeeperHostname();
            final int zookeeperPort = getZookeeperPort();
            final HBaseConfiguration configuration = new HBaseConfiguration(zookeeperHostname, zookeeperPort,
                    ColumnType.VARCHAR);
            _dataContext = new HBaseDataContext(configuration);
            createTableNatively();
        }
    }

    public void testCreateInsertQueryAndDrop() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        // test the schema exploration
        final Table table = _dataContext.getDefaultSchema().getTableByName(EXAMPLE_TABLE_NAME);
        assertNotNull(table);

        assertEquals("[" + HBaseDataContext.FIELD_ID + ", " + CF_BAR + ", " + CF_FOO + "]", Arrays.toString(table
                .getColumnNames()
                .toArray()));
        assertEquals(ColumnType.MAP, table.getColumn(1).getType());

        // insert two records
        insertRecordsNatively();

        // query using regular configuration
        final DataSet dataSet1 = _dataContext.query().from(EXAMPLE_TABLE_NAME).selectAll().execute();
        try {
            assertTrue(dataSet1.next());
            assertEquals("Row[values=[" + RK_1 + ", {" + Q_HEY + "=" + V_YO + "," + Q_HI + "=" + V_THERE + "}, {"
                    + Q_HELLO + "=" + V_WORLD + "}]]", dataSet1.getRow().toString());
            assertTrue(dataSet1.next());
            assertEquals("Row[values=[" + RK_2 + ", {" + Q_BAH + "=" + new String(V_123_BYTE_ARRAY) + "," + Q_HI + "="
                    + V_YOU + "}, {}]]", dataSet1.getRow().toString());
            assertFalse(dataSet1.next());
        } finally {
            dataSet1.close();
        }

        // query using custom table definitions
        final String columnName1 = CF_FOO;
        final String columnName2 = CF_BAR + ":" + Q_HI;
        final String columnName3 = CF_BAR + ":" + Q_HEY;
        final String[] columnNames = new String[] { columnName1, columnName2, columnName3 };
        final ColumnType[] columnTypes = new ColumnType[] { ColumnType.MAP, ColumnType.VARCHAR, ColumnType.VARCHAR };
        final SimpleTableDef[] tableDefinitions = new SimpleTableDef[] { new SimpleTableDef(EXAMPLE_TABLE_NAME,
                columnNames, columnTypes) };
        _dataContext = new HBaseDataContext(new HBaseConfiguration("SCH", getZookeeperHostname(), getZookeeperPort(),
                tableDefinitions, ColumnType.VARCHAR));

        final DataSet dataSet2 = _dataContext
                .query()
                .from(EXAMPLE_TABLE_NAME)
                .select(columnName1, columnName2, columnName3)
                .execute();
        try {
            assertTrue(dataSet2.next());
            assertEquals("Row[values=[{" + Q_HELLO + "=" + V_WORLD + "}, " + V_THERE + ", " + V_YO + "]]", dataSet2
                    .getRow()
                    .toString());
            assertTrue(dataSet2.next());
            assertEquals("Row[values=[{}, " + V_YOU + ", null]]", dataSet2.getRow().toString());
            assertFalse(dataSet2.next());
        } finally {
            dataSet2.close();
        }

        // query count
        final DataSet dataSet3 = _dataContext.query().from(EXAMPLE_TABLE_NAME).selectCount().execute();
        try {
            assertTrue(dataSet3.next());
            assertEquals("Row[values=[" + NUMBER_OF_ROWS + "]]", dataSet3.getRow().toString());
            assertFalse(dataSet3.next());
        } finally {
            dataSet3.close();
        }

        // query only id
        final DataSet dataSet4 = _dataContext
                .query()
                .from(EXAMPLE_TABLE_NAME)
                .select(HBaseDataContext.FIELD_ID)
                .execute();

        try {
            assertTrue(dataSet4.next());
            assertEquals("Row[values=[" + RK_1 + "]]", dataSet4.getRow().toString());
            assertTrue(dataSet4.next());
            assertEquals("Row[values=[" + RK_2 + "]]", dataSet4.getRow().toString());
            assertFalse(dataSet4.next());
        } finally {
            dataSet4.close();
        }

        // primary key lookup query - using GET
        final DataSet dataSet5 = _dataContext
                .query()
                .from(EXAMPLE_TABLE_NAME)
                .select(HBaseDataContext.FIELD_ID)
                .where(HBaseDataContext.FIELD_ID)
                .eq(RK_1)
                .execute();

        try {
            assertTrue(dataSet5.next());
            assertEquals("Row[values=[" + RK_1 + "]]", dataSet5.getRow().toString());
            assertFalse(dataSet5.next());
        } finally {
            dataSet5.close();
        }
    }

    private void insertRecordsNatively() throws Exception {
        final org.apache.hadoop.hbase.client.Table hTable = _dataContext.getHTable(EXAMPLE_TABLE_NAME);
        try {
            final Put put1 = new Put(RK_1.getBytes());
            put1.addColumn(CF_FOO.getBytes(), Q_HELLO.getBytes(), V_WORLD.getBytes());
            put1.addColumn(CF_BAR.getBytes(), Q_HI.getBytes(), V_THERE.getBytes());
            put1.addColumn(CF_BAR.getBytes(), Q_HEY.getBytes(), V_YO.getBytes());

            final Put put2 = new Put(RK_2.getBytes());
            put2.addColumn(CF_BAR.getBytes(), Q_BAH.getBytes(), V_123_BYTE_ARRAY);
            put2.addColumn(CF_BAR.getBytes(), Q_HI.getBytes(), V_YOU.getBytes());

            final Object[] result = new Object[NUMBER_OF_ROWS];
            hTable.batch(Arrays.asList(put1, put2), result);
        } finally {
            hTable.close();
        }
    }

    private void createTableNatively() throws Exception {
        final TableName tableName = TableName.valueOf(EXAMPLE_TABLE_NAME);

        // check if the table exists
        if (_dataContext.getAdmin().isTableAvailable(tableName)) {
            System.out.println("Unittest table already exists: " + EXAMPLE_TABLE_NAME);
            // table already exists
            return;
        }

        Admin admin = _dataContext.getAdmin();
        System.out.println("Creating table");
        final HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor(CF_FOO.getBytes()));
        tableDescriptor.addFamily(new HColumnDescriptor(CF_BAR.getBytes()));
        admin.createTable(tableDescriptor);
        System.out.println("Created table");
    }
}
