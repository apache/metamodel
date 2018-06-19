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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.metamodel.MetaModelException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DropTableTest extends HBaseUpdateCallbackTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
     * Check if drop table is supported
     */
    @Test
    public void testDropTableSupported() {
        assertTrue(getUpdateCallback().isDropTableSupported());
    }

    /**
     * Trying to drop a table, that doesn't exist in the datastore, should throw a exception
     */
    @Test
    public void testDropTableThatDoesntExist() {
        exception.expect(MetaModelException.class);
        exception.expectMessage("Trying to delete a table that doesn't exist in the datastore.");

        final HBaseTable table = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR);
        getUpdateCallback().dropTable(table).execute();
    }

    /**
     * Goodflow. Dropping a table successfully.
     *
     * @throws IOException
     */
    @Test
    public void testDropTableSuccesfully() throws IOException {
        final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                CF_BAR);
        getUpdateCallback().dropTable(existingTable).execute();
        try (final Admin admin = getDataContext().getAdmin()) {
            assertFalse(admin.tableExists(TableName.valueOf(TABLE_NAME)));
        }
    }
}
