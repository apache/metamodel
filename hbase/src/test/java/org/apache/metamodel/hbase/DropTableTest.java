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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.metamodel.MetaModelException;

public class DropTableTest extends HBaseUpdateCallbackTest {

    /**
     * Check if drop table is supported
     */
    public void testDropTableSupported() {
        assertTrue(getUpdateCallback().isDropTableSupported());
    }

    /**
     * Trying to drop a table, that doesn't exist in the datastore, should throw a exception
     */
    public void testDropTableThatDoesntExist() {
        if (isConfigured()) {
            try {
                final HBaseTable table = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, null);
                getUpdateCallback().dropTable(table).execute();
                fail("Should get an exception that the table doesn't exist in the datastore");
            } catch (MetaModelException e) {
                assertEquals("Trying to delete a table that doesn't exist in the datastore.", e.getMessage());
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
            new HBaseClient(getDataContext().getConnection()).dropTable(null);
            fail("Should get an exception that tableName is null");
        } catch (IllegalArgumentException e) {
            assertEquals("Can't drop a table without having the tableName", e.getMessage());
        }
    }

    /**
     * Goodflow. Droping a table succesfully.
     * @throws IOException
     */
    public void testDropTableSuccesfully() throws IOException {
        if (isConfigured()) {
            try {
                final HBaseTable existingTable = createAndInsertTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                        CF_BAR);
                getUpdateCallback().dropTable(existingTable).execute();
                try (final Admin admin = getDataContext().getAdmin()) {
                    assertFalse(admin.tableExists(TableName.valueOf(TABLE_NAME)));
                }
            } catch (Exception e) {
                fail("Should not get an exception that the table doesn't exist in the datastore");
            }
        } else {
            warnAboutANotExecutedTest(getClass().getName(), new Object() {
            }.getClass().getEnclosingMethod().getName());
        }
    }
}
