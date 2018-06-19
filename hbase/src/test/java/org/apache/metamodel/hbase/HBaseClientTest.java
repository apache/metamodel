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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HBaseClientTest extends HBaseUpdateCallbackTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    /* Inserting a row */

    /**
     * Inserting a row with the columns null, should throw a exception
     */
    @Test
    public void testInsertRowWithColumnsNull() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn");

        final Object[] values = new String[] { "Values" };
        new HBaseClient(getDataContext().getConnection()).insertRow("tableName", null, values, 0);
    }

    /**
     * Inserting a row with with the values null, should throw a exception
     */
    @Test
    public void testInsertRowWithValuesNull() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn");

        final HBaseTable table = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR);
        final Map<HBaseColumn, Object> row = createRow(table, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, false);
        final HBaseColumn[] columns = convertToHBaseColumnsArray(getHBaseColumnsFromRow(row));
        new HBaseClient(getDataContext().getConnection()).insertRow(table.getName(), columns, null, 0);
    }

    /**
     * Inserting a row with with the indexOfIdColumn out of bounce, should throw a exception
     */
    @Test
    public void testInsertRowWithIndexOfIdColumnOutOfBounce() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn");

        final HBaseTable table = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR);
        final Map<HBaseColumn, Object> row = createRow(table, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, false);
        final HBaseColumn[] columns = convertToHBaseColumnsArray(getHBaseColumnsFromRow(row));
        final Object[] values = new String[] { "Values" };
        new HBaseClient(getDataContext().getConnection()).insertRow(table.getName(), columns, values, 10);
    }

    /**
     * Inserting a row with with the rowKey null, should throw a exception
     */
    @Test
    public void testInsertRowWithRowKeyNull() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn");

        final HBaseTable table = createHBaseTable(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR);
        final Map<HBaseColumn, Object> row = createRow(table, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, false);
        final HBaseColumn[] columns = convertToHBaseColumnsArray(getHBaseColumnsFromRow(row));
        final Object[] values = new String[] { null };
        new HBaseClient(getDataContext().getConnection()).insertRow(table.getName(), columns, values, 0);
    }

    /* Creating a table */

    /**
     * Creating a table with the tableName null, should throw a exception
     */
    @Test
    public void testCreateTableWithTableNameNull() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Can't create a table without having the tableName or columnFamilies");

        final Set<String> columnFamilies = new LinkedHashSet<>();
        columnFamilies.add("1");
        new HBaseClient(getDataContext().getConnection()).createTable(null, columnFamilies);
    }

    /**
     * Creating a table with the columnFamilies null, should throw a exception
     */
    @Test
    public void testCreateTableWithColumnFamiliesNull() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Can't create a table without having the tableName or columnFamilies");

        new HBaseClient(getDataContext().getConnection()).createTable("1", null);
    }

    /**
     * Creating a table with the columnFamilies empty, should throw a exception
     */
    @Test
    public void testCreateTableWithColumnFamiliesEmpty() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Can't create a table without having the tableName or columnFamilies");

        final Set<String> columnFamilies = new LinkedHashSet<>();
        new HBaseClient(getDataContext().getConnection()).createTable("1", columnFamilies);
    }

    /* Deleting a row */

    /**
     * Deleting a row with the tableName null, should throw a exception
     */
    @Test
    public void testDeleteRowWithTableNameNull() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Can't delete a row without having tableName or rowKey");

        new HBaseClient(getDataContext().getConnection()).deleteRow(null, new String("1"));
    }

    /**
     * Deleting a row with the rowKey null, should throw a exception
     */
    @Test
    public void testDeleteRowWithRowKeyNull() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Can't delete a row without having tableName or rowKey");

        new HBaseClient(getDataContext().getConnection()).deleteRow("tableName", null);
    }

    /* Dropping/deleting a table */

    /**
     * Dropping a table with the tableName null, should throw a exception
     */
    @Test
    public void testDropTableWithTableNameNull() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Can't drop a table without having the tableName");

        new HBaseClient(getDataContext().getConnection()).dropTable(null);
    }
}
