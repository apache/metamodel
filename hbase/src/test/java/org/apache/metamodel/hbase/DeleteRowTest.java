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
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.schema.MutableTable;

public class DeleteRowTest extends HBaseUpdateCallbackTest {

    /**
     * Delete is supported
     */
    public void testDeleteSupported() {
        assertTrue(getUpdateCallback().isDeleteSupported());
    }

    /**
     * Having the table type wrong, should throw an exception
     */
    public void testTableWrongType() {
        final MutableTable mutableTable = new MutableTable();
        try {
            getUpdateCallback().deleteFrom(mutableTable);
            fail("Should get an exception that the type of the table is wrong.");
        } catch (IllegalArgumentException e) {
            assertEquals("Not an HBase table: " + mutableTable, e.getMessage());
        }
    }

    /**
     * Creating a HBaseRowDeletionBuilder with the hBaseClient null, should throw an exception
     * @throws IOException 
     */
    public void testHBaseClientNullAtBuilder() throws IOException {
        if (isConfigured()) {
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                new HBaseRowDeletionBuilder(null, existingTable);
                fail("Should get an exception that hBaseClient can't be null.");
            } catch (IllegalArgumentException e) {
                assertEquals("hBaseClient cannot be null", e.getMessage());
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * Not setting the rowkey, should throw an exception
     * @throws IOException 
     */
    public void testNotSettingRowkey() throws IOException {
        if (isConfigured()) {
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                getUpdateCallback().deleteFrom(existingTable).execute();
                fail("Should get an exception that the columnFamily doesn't exist.");
            } catch (MetaModelException e) {
                assertEquals("Key cannot be null", e.getMessage());
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
        try {
            new HBaseClient(getDataContext().getConnection()).deleteRow(null, new String("1"));
            fail("Should get an exception that tableName is null");
        } catch (IllegalArgumentException e) {
            assertEquals("Can't delete a row without having tableName or rowKey", e.getMessage());
        }
    }

    /**
     * Creating a HBaseClient with the rowKey null, should throw a exception
     */
    public void testCreatingTheHBaseClientWithRowKeyNull() {
        try {
            new HBaseClient(getDataContext().getConnection()).deleteRow("tableName", null);
            fail("Should get an exception that rowKey is null");
        } catch (IllegalArgumentException e) {
            assertEquals("Can't delete a row without having tableName or rowKey", e.getMessage());
        }
    }

    /**
     * Goodflow. Deleting a row, that doesn't exist, should not throw an exception
     */
    public void testDeletingNotExistingRow() {
        if (isConfigured()) {
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);

                checkRows(false);
                final HBaseRowDeletionBuilder rowDeletionBuilder = (HBaseRowDeletionBuilder) getUpdateCallback()
                        .deleteFrom(existingTable);
                rowDeletionBuilder.setKey(RK_1);
                rowDeletionBuilder.execute();
                checkRows(false);
            } catch (Exception e) {
                fail("Should not get an exception that the row doesn't exist.");
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }

    /**
     * Goodflow. Deleting a row succesfully.
     */
    public void testDeleteRowSuccesfully() {
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
                final HBaseRowDeletionBuilder rowDeletionBuilder = (HBaseRowDeletionBuilder) getUpdateCallback()
                        .deleteFrom(existingTable);
                rowDeletionBuilder.setKey(RK_1);
                rowDeletionBuilder.execute();
                checkRows(false);
            } catch (Exception e) {
                fail("Should not get an exception on deleting a row.");
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }
}
