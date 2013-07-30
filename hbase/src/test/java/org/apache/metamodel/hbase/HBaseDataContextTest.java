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

import junit.framework.TestCase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;

public class HBaseDataContextTest extends TestCase {

    private static final String EXAMPLE_TABLE_NAME = "table_for_junit";

    private final String hostname = HBaseConfiguration.DEFAULT_ZOOKEEPER_HOSTNAME;
    private final int port = HBaseConfiguration.DEFAULT_ZOOKEEPER_PORT;

    private HBaseDataContext _dataContext;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        _dataContext = new HBaseDataContext(new HBaseConfiguration(hostname, port, ColumnType.VARCHAR));
        createTableNatively();
    }

    public void testCreateInsertQueryAndDrop() throws Exception {
        // test the schema exploration
        final Table table = _dataContext.getDefaultSchema().getTableByName(EXAMPLE_TABLE_NAME);
        assertNotNull(table);

        assertEquals("[_id, bar, foo]", Arrays.toString(table.getColumnNames()));
        assertEquals(ColumnType.MAP, table.getColumn(1).getType());

        // insert two records
        insertRecordsNatively();

        // query using regular configuration
        final DataSet dataSet1 = _dataContext.query().from(EXAMPLE_TABLE_NAME).selectAll().execute();
        try {
            assertTrue(dataSet1.next());
            assertEquals(
                    "Row[values=[junit1, {[104, 101, 121]=[121, 111],[104, 105]=[116, 104, 101, 114, 101]}, {[104, 101, 108, 108, 111]=[119, 111, 114, 108, 100]}]]",
                    dataSet1.getRow().toString());
            assertTrue(dataSet1.next());
            assertEquals("Row[values=[junit2, {[98, 97, 104]=[1, 2, 3],[104, 105]=[121, 111, 117]}, {}]]", dataSet1
                    .getRow().toString());
            assertFalse(dataSet1.next());
        } finally {
            dataSet1.close();
        }

        // query using custom table definitions
        final String[] columnNames = new String[] { "foo", "bar:hi", "bar:hey" };
        final ColumnType[] columnTypes = new ColumnType[] { ColumnType.MAP, ColumnType.VARCHAR, ColumnType.VARCHAR };
        final SimpleTableDef[] tableDefinitions = new SimpleTableDef[] { new SimpleTableDef(EXAMPLE_TABLE_NAME,
                columnNames, columnTypes) };
        _dataContext = new HBaseDataContext(new HBaseConfiguration("SCH", hostname, port, tableDefinitions,
                ColumnType.VARCHAR));

        final DataSet dataSet2 = _dataContext.query().from(EXAMPLE_TABLE_NAME).select("foo", "bar:hi", "bar:hey")
                .execute();
        try {
            assertTrue(dataSet2.next());
            assertEquals("Row[values=[{[104, 101, 108, 108, 111]=[119, 111, 114, 108, 100]}, there, yo]]", dataSet2
                    .getRow().toString());
            assertTrue(dataSet2.next());
            assertEquals("Row[values=[{}, you, null]]", dataSet2.getRow().toString());
            assertFalse(dataSet2.next());
        } finally {
            dataSet2.close();
        }

        // query count
        final DataSet dataSet3 = _dataContext.query().from(EXAMPLE_TABLE_NAME).selectCount().execute();
        try {
            assertTrue(dataSet3.next());
            assertEquals("Row[values=[2]]", dataSet3.getRow().toString());
            assertFalse(dataSet3.next());
        } finally {
            dataSet3.close();
        }

        // query only id
        final DataSet dataSet4 = _dataContext.query().from(EXAMPLE_TABLE_NAME)
                .select(HBaseDataContext.FIELD_ID).execute();
        try {
            assertTrue(dataSet4.next());
            assertEquals("Row[values=[junit1]]", dataSet4.getRow().toString());
            assertTrue(dataSet4.next());
            assertEquals("Row[values=[junit2]]", dataSet4.getRow().toString());
            assertFalse(dataSet4.next());
        } finally {
            dataSet4.close();
        }
    }

    private void insertRecordsNatively() throws Exception {
        final HTablePool tablePool = _dataContext.getTablePool();
        final HTableInterface hTable = tablePool.getTable(EXAMPLE_TABLE_NAME);
        try {
            final Put put1 = new Put("junit1".getBytes());
            put1.add("foo".getBytes(), "hello".getBytes(), "world".getBytes());
            put1.add("bar".getBytes(), "hi".getBytes(), "there".getBytes());
            put1.add("bar".getBytes(), "hey".getBytes(), "yo".getBytes());

            final Put put2 = new Put("junit2".getBytes());
            put2.add("bar".getBytes(), "bah".getBytes(), new byte[] { 1, 2, 3 });
            put2.add("bar".getBytes(), "hi".getBytes(), "you".getBytes());

            hTable.batch(Arrays.asList(put1, put2));
        } finally {
            hTable.close();
            tablePool.closeTablePool(EXAMPLE_TABLE_NAME);
            tablePool.close();
        }
    }

    private void createTableNatively() throws Exception {
        // check if the table exists
        if (_dataContext.getHBaseAdmin().isTableAvailable(EXAMPLE_TABLE_NAME)) {
            // table already exists
            return;
        }

        HBaseAdmin admin = _dataContext.getHBaseAdmin();
        System.out.println("Creating table");
        final HTableDescriptor tableDescriptor = new HTableDescriptor(EXAMPLE_TABLE_NAME.getBytes());
        tableDescriptor.addFamily(new HColumnDescriptor("foo".getBytes()));
        tableDescriptor.addFamily(new HColumnDescriptor("bar".getBytes()));
        admin.createTable(tableDescriptor);
        System.out.println("Created table");
    }
}
