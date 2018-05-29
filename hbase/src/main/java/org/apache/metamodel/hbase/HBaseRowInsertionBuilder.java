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

import java.util.List;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.insert.AbstractRowInsertionBuilder;
import org.apache.metamodel.schema.Column;

/**
 * A builder-class to insert rows in a HBase datastore.
 */
// TODO: Possible future improvement: Make it possible to change the columns for each execute.
// Now each row will get exactly the same columns.
public class HBaseRowInsertionBuilder extends AbstractRowInsertionBuilder<HBaseUpdateCallback> {
    final Integer _indexOfIdColumn;

    /**
     * Creates a {@link HBaseRowInsertionBuilder}. The table and the column's columnFamilies are checked to exist in the schema.
     * @param updateCallback
     * @param table
     * @param columns
     * @throws IllegalArgumentException the columns list can't be null or empty
     * @throws MetaModelException when no ID-column is found.
     */
    public HBaseRowInsertionBuilder(final HBaseUpdateCallback updateCallback, final HBaseTable table,
            final List<Column> columns) {
        super(updateCallback, table, columns);
        if (columns.isEmpty()) { // TODO: Columns null will already result in a NullPointer at the super. Should the
                                 // super get a extra check?
            throw new IllegalArgumentException("The hbaseColumns list is null or empty");
        }

        this._indexOfIdColumn = HBaseColumn.findIndexOfIdColumn(HBaseColumn.convertToHBaseColumnsList(columns));
        if (_indexOfIdColumn == null) {
            throw new MetaModelException("The ID-Column was not found");
        }

        checkTable(updateCallback, table);
        table.checkForNotMatchingColumnFamilies(HBaseColumn.getColumnFamilies(HBaseColumn.convertToHBaseColumnsList(
                columns)));
    }

    /**
     * Check if the table and it's columnFamilies exist in the schema
     * @param updateCallback
     * @param tableGettingInserts
     * @throws MetaModelException If the table or the columnFamilies don't exist
     */
    private void checkTable(final HBaseUpdateCallback updateCallback, final HBaseTable tableGettingInserts) {
        final HBaseTable tableInSchema = (HBaseTable) updateCallback.getDataContext().getDefaultSchema().getTableByName(
                tableGettingInserts.getName());
        if (tableInSchema == null) {
            throw new MetaModelException("Trying to insert data into table: " + tableGettingInserts.getName()
                    + ", which doesn't exist yet");
        }
        tableInSchema.checkForNotMatchingColumnFamilies(HBaseColumn.getColumnFamilies(tableGettingInserts
                .getHBaseColumnsInternal()));
    }

    @Override
    public synchronized void execute() {
        getUpdateCallback().getHBaseClient().insertRow(getTable().getName(), getColumns(), getValues(), _indexOfIdColumn
                .intValue());
    }

    @Override
    public HBaseColumn[] getColumns() {
        return HBaseColumn.convertToHBaseColumnsArray(super.getColumns());
    }
}
