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
import java.util.Arrays;
import java.util.List;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.insert.AbstractRowInsertionBuilder;
import org.apache.metamodel.schema.Column;

/**
 * A builder-class to insert rows in a HBase datastore
 */
public class HBaseRowInsertionBuilder extends AbstractRowInsertionBuilder<HBaseUpdateCallback> {
    public HBaseRowInsertionBuilder(final HBaseUpdateCallback updateCallback, final HBaseTable table,
            final List<Column> columns) {
        super(updateCallback, table, columns);
        checkTable(updateCallback, table);
    }

    /**
     * Check if the table exits and it's columnFamilies exist
     * If the table doesn't exist, then a {@link MetaModelException} is thrown
     * @param updateCallback
     * @param tableGettingInserts
     */
    private void checkTable(final HBaseUpdateCallback updateCallback, final HBaseTable tableGettingInserts) {
        final HBaseTable tableInSchema = (HBaseTable) updateCallback.getDataContext().getDefaultSchema().getTableByName(
                tableGettingInserts.getName());
        if (tableInSchema == null) {
            throw new MetaModelException("Trying to insert data into table: " + tableGettingInserts.getName()
                    + ", which doesn't exist yet");
        }
        tableInSchema.checkForNotMatchingColumns(tableGettingInserts.getColumnNames());
    }

    @Override
    public synchronized void execute() {
        if (getColumns() == null || getColumns().length == 0) {
            throw new MetaModelException("The hbaseColumns-array is null or empty");
        }
        if (getValues() == null || getValues().length == 0) {
            throw new MetaModelException("The values-array is null or empty");
        }
        try {
            final HBaseClient hBaseClient = getUpdateCallback().getHBaseClient();
            hBaseClient.writeRow((HBaseTable) getTable(), getColumns(), getValues());
        } catch (IOException e) {
            throw new MetaModelException(e);
        }
    }

    @Override
    public HBaseColumn[] getColumns() {
        return Arrays.stream(super.getColumns()).map(column -> (HBaseColumn) column).toArray(
                size -> new HBaseColumn[size]);
    }
}
