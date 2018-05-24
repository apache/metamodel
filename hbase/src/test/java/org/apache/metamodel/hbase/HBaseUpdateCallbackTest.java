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
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ImmutableSchema;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;

public class HBaseUpdateCallbackTest extends HBaseTestCase {

    private HBaseUpdateCallback updateCallback;
    private MutableSchema schema;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        if (isConfigured()) {
            updateCallback = new HBaseUpdateCallback(getDataContext());
            schema = (MutableSchema) getDataContext().getDefaultSchema();

            if (schema.getTableByName(TABLE_NAME) != null) {
                dropTableIfItExists();
            }
        }
    }

    public void testDropTable() throws IOException {
        dropTableIfItExists();

        try {
            HBaseTable table = createHBaseTable();
            updateCallback.dropTable(table).execute();
            fail("Should get an exception that the table doesn't exist in the datastore");
        } catch (MetaModelException e) {
            assertEquals("Trying to delete a table that doesn't exist in the datastore.", e.getMessage());
        }
    }

    private void dropTableIfItExists() {
        Table table = schema.getTableByName(TABLE_NAME);
        if (table != null) {
            updateCallback.dropTable(table).execute();
            // Check schema
            assertNull(schema.getTableByName(TABLE_NAME));
            // Check in the datastore
            try (Admin admin = getDataContext().getAdmin()) {
                assertFalse(admin.tableExists(TableName.valueOf(TABLE_NAME)));
            } catch (IOException e) {
                fail("Should not an exception checking if the table exists");
            }
        }
    }

    public void testCreateTable() {
        // Drop the table if it exists
        dropTableIfItExists();

        // Test 1: Create a table with an immutableSchema, should throw a IllegalArgumentException
        ImmutableSchema immutableSchema = new ImmutableSchema(schema);
        try {
            updateCallback.createTable(immutableSchema, TABLE_NAME).execute();
            fail("Should get an exception that the schema isn't mutable");
        } catch (IllegalArgumentException e) {
            assertEquals("Not a mutable schema: " + immutableSchema, e.getMessage());
        }

        // Test 2: Create a table without columnFamilies, should throw a MetaModelException
        try {
            updateCallback.createTable(schema, TABLE_NAME).execute();
            fail("Should get an exception that the columnFamilies haven't been set");
        } catch (MetaModelException e) {
            assertEquals("Creating a table without columnFamilies", e.getMessage());
        }

        // Test 3: Create a table with columnFamilies null, should throw a MetaModelException
        try {
            updateCallback.createTable(schema, TABLE_NAME, null).execute();
            fail("Should get an exception that the columnFamilies haven't been set");
        } catch (MetaModelException e) {
            assertEquals("Creating a table without columnFamilies", e.getMessage());
        }

        // Test 4: Create a table with columnFamilies empty, should throw a MetaModelException
        try {
            final LinkedHashSet<String> columnFamilies = new LinkedHashSet<String>();
            updateCallback.createTable(schema, TABLE_NAME, columnFamilies).execute();
            fail("Should get an exception that the columnFamilies haven't been set");
        } catch (MetaModelException e) {
            assertEquals("Creating a table without columnFamilies", e.getMessage());
        }

        HBaseTable table = createHBaseTable();

        // Test 5: Create a table without the ID-Column, should throw a MetaModelException
        ArrayList<HBaseColumn> hBaseColumnsAsArrayList = createListWithHBaseColumnsExcludingIDColumn(table);
        HBaseColumn[] hBaseColumnsAsArray = convertToHBaseColumnArray(hBaseColumnsAsArrayList);
        Set<String> columnFamilies = HBaseColumn.getColumnFamilies(hBaseColumnsAsArray);
        try {
            HBaseCreateTableBuilder hBaseCreateTableBuilder = (HBaseCreateTableBuilder) updateCallback.createTable(
                    schema, TABLE_NAME);

            hBaseCreateTableBuilder.setColumnFamilies(columnFamilies);
            hBaseCreateTableBuilder.execute();
            fail("Should get an exception that the ID-colum is missing");
        } catch (MetaModelException e) {
            assertEquals("ColumnFamily: " + HBaseDataContext.FIELD_ID + " not found", e.getMessage());
        }

        // Test 6: Create a table including the ID-Column (columnFamilies not in constructor), should work
        hBaseColumnsAsArrayList = createListWithHBaseColumnsIncludingIDColumn(table);
        hBaseColumnsAsArray = convertToHBaseColumnArray(hBaseColumnsAsArrayList);
        columnFamilies = HBaseColumn.getColumnFamilies(hBaseColumnsAsArray);
        try {
            HBaseCreateTableBuilder hBaseCreateTableBuilder = (HBaseCreateTableBuilder) updateCallback.createTable(
                    schema, TABLE_NAME);

            hBaseCreateTableBuilder.setColumnFamilies(HBaseColumn.getColumnFamilies(hBaseColumnsAsArray));
            hBaseCreateTableBuilder.execute();
            checkSuccesfullyInsertedTable();
        } catch (Exception e) {
            fail("Should not get an exception");
        }
        dropTableIfItExists();

        // Test 7: Create a table including the ID-Column (columnFamilies in constructor), should work
        try {
            updateCallback.createTable(schema, TABLE_NAME, columnFamilies).execute();
            checkSuccesfullyInsertedTable();
        } catch (Exception e) {
            fail("Should not get an exception");
        }
        dropTableIfItExists();
    }

    private void checkSuccesfullyInsertedTable() throws IOException {
        // Check the schema
        assertNotNull(schema.getTableByName(TABLE_NAME));
        // Check in the datastore
        try (Admin admin = getDataContext().getAdmin()) {
            assertTrue(admin.tableExists(TableName.valueOf(TABLE_NAME)));
        } catch (IOException e) {
            fail("Should not an exception checking if the table exists");
        }
    }

    // public void testInsertRows() throws IOException {
    // // Drop the table if it exists
    // dropTableIfItExists();
    //
    // insertTable();
    // }

    private void insertTable() throws IOException {
        HBaseTable table = createHBaseTable();
        ArrayList<HBaseColumn> hBaseColumnsAsArrayList = createListWithHBaseColumnsIncludingIDColumn(table);
        HBaseColumn[] hBaseColumnsAsArray = convertToHBaseColumnArray(hBaseColumnsAsArrayList);
        Set<String> columnFamilies = HBaseColumn.getColumnFamilies(hBaseColumnsAsArray);
        updateCallback.createTable(schema, TABLE_NAME, columnFamilies).execute();
        checkSuccesfullyInsertedTable();
    }

    private HBaseTable createHBaseTable() {
        String[] columnNames = new String[] { CF_FOO, CF_BAR };
        ColumnType[] columnTypes = new ColumnType[] { ColumnType.STRING, ColumnType.STRING };
        SimpleTableDef tableDef = new SimpleTableDef(TABLE_NAME, columnNames, columnTypes);
        return new HBaseTable(getDataContext(), tableDef, schema, ColumnType.STRING);
    }

    private static ArrayList<HBaseColumn> createListWithHBaseColumnsExcludingIDColumn(final HBaseTable table) {
        ArrayList<HBaseColumn> hbaseColumns = new ArrayList<HBaseColumn>();
        hbaseColumns.add(new HBaseColumn(CF_FOO, Q_HELLO, table));
        hbaseColumns.add(new HBaseColumn(CF_FOO, Q_HI, table));
        hbaseColumns.add(new HBaseColumn(CF_BAR, Q_HEY, table));
        hbaseColumns.add(new HBaseColumn(CF_BAR, Q_BAH, table));
        return hbaseColumns;
    }

    private static ArrayList<HBaseColumn> createListWithHBaseColumnsIncludingIDColumn(final HBaseTable table) {
        ArrayList<HBaseColumn> hbaseColumns = createListWithHBaseColumnsExcludingIDColumn(table);
        hbaseColumns.add(new HBaseColumn(HBaseDataContext.FIELD_ID, table));
        return hbaseColumns;
    }

    private static HBaseColumn[] convertToHBaseColumnArray(final ArrayList<HBaseColumn> hBaseColumnsAsArrayList) {
        return hBaseColumnsAsArrayList.toArray(new HBaseColumn[hBaseColumnsAsArrayList.size()]);
    }
}
