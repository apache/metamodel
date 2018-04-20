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

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.insert.AbstractRowInsertionBuilder;
import org.apache.metamodel.schema.Table;

public class HBaseRowInsertionBuilder extends AbstractRowInsertionBuilder<HBaseUpdateCallback> {

    private final HBaseColumn[] _outputColumns;

    public HBaseRowInsertionBuilder(HBaseUpdateCallback updateCallback, Table table, HBaseColumn[] outputColumns) {
        super(updateCallback, table, outputColumns.length);
        _outputColumns = outputColumns;
    }

    @Override
    public void execute() throws MetaModelException {
        checkForMatchingColumnFamilies(getTable(), _outputColumns);
        getUpdateCallback().writeRow((HBaseTable) getTable(), _outputColumns, getValues());
    }

    private void checkForMatchingColumnFamilies(Table table, HBaseColumn[] outputColumns) {
        for (int i = 0; i < outputColumns.length; i++) {
            if (!outputColumns[i].getColumnFamily().equals(HBaseDataContext.FIELD_ID)) {
                boolean matchingColumnFound = false;
                int indexOfTablesColumn = 0;

                while (!matchingColumnFound && indexOfTablesColumn < table.getColumnCount()) {
                    if (outputColumns[i].getColumnFamily().equals(table.getColumn(indexOfTablesColumn).getName())) {
                        matchingColumnFound = true;
                    } else {
                        indexOfTablesColumn++;
                    }
                }

                if (!matchingColumnFound) {
                    throw new IllegalArgumentException(String.format(
                            "OutputColumnFamily: %s doesn't exist in the schema of the table", outputColumns[i]
                                    .getColumnFamily()));
                }
            }
        }
    }

    public HBaseColumn[] getOutputColumns() {
        return _outputColumns;
    }

    public void setOutputColumns(HBaseColumn[] outputColumns) {
        if (outputColumns.length != _outputColumns.length) {
            throw new IllegalArgumentException("The amount of outputColumns don't match");
        }
        for (int i = 0; i < outputColumns.length; i++) {
            _outputColumns[i] = outputColumns[i];
        }
    }

}
