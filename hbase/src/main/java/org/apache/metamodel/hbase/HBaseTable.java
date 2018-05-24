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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.SimpleTableDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table implementation for HBase
 */
final class HBaseTable extends MutableTable {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(HBaseTable.class);

    private final transient HBaseDataContext _dataContext;
    private final transient ColumnType _defaultRowKeyColumnType;

    /**
     * Creates an HBaseTable. If the tableDef variable doesn't include the ID column (see {@link HBaseDataContext#FIELD_ID}). 
     * Then it's first inserted.
     * @param dataContext
     * @param tableDef Table definition. The tableName, columnNames and columnTypes variables are used.
     * @param schema {@link MutableSchema} where the table belongs to.
     * @param defaultRowKeyColumnType This variable determines the {@link ColumnType}, 
     * used when the tableDef doesn't include the ID column (see {@link HBaseDataContext#FIELD_ID}). 
     */
    public HBaseTable(HBaseDataContext dataContext, SimpleTableDef tableDef, MutableSchema schema,
            ColumnType defaultRowKeyColumnType) {
        super(tableDef.getName(), TableType.TABLE, schema);
        _dataContext = dataContext;
        _defaultRowKeyColumnType = defaultRowKeyColumnType;

        // Add the columns
        final String[] columnNames = tableDef.getColumnNames();
        if (columnNames == null || columnNames.length == 0) {
            logger.info("No user-defined columns specified for table {}. Columns will be auto-detected.");
        } else {
            final ColumnType[] columnTypes = tableDef.getColumnTypes();

            // Find the ID-Column
            boolean idColumnFound = false;
            int indexOfIDColumn = 0;
            while (!idColumnFound && indexOfIDColumn < columnNames.length) {
                if (columnNames[indexOfIDColumn].equals(HBaseDataContext.FIELD_ID)) {
                    idColumnFound = true;
                } else {
                    indexOfIDColumn++;
                }
            }

            int columnNumber = indexOfIDColumn + 1; // ColumnNumbers start from 1

            // Add the ID-Column, even if the column wasn't included in columnNames
            ColumnType columnType;
            if (idColumnFound) {
                columnType = columnTypes[indexOfIDColumn];
            } else {
                columnType = defaultRowKeyColumnType;
            }
            final MutableColumn idColumn = new MutableColumn(HBaseDataContext.FIELD_ID, columnType)
                    .setPrimaryKey(true)
                    .setColumnNumber(columnNumber)
                    .setTable(this);
            addColumn(idColumn);

            // Add the other columns
            for (int i = 0; i < columnNames.length; i++) {
                final String columnName = columnNames[i];
                if (idColumnFound) {
                    columnNumber = i + 1; // ColumnNumbers start from 1
                } else {
                    columnNumber = i + 2; // ColumnNumbers start from 1 + the ID-column has just been created
                }
                if (!HBaseDataContext.FIELD_ID.equals(columnName)) {
                    final ColumnType type = columnTypes[i];
                    final MutableColumn column = new MutableColumn(columnName, type);
                    column.setTable(this);
                    column.setColumnNumber(columnNumber);
                    addColumn(column);
                    columnNumber++;
                }
            }
        }
    }

    @Override
    protected synchronized List<Column> getColumnsInternal() {
        final List<Column> columnsInternal = super.getColumnsInternal();
        if (columnsInternal.isEmpty() && _dataContext != null) {
            try {
                final org.apache.hadoop.hbase.client.Table table = _dataContext.getHTable(getName());
                int columnNumber = 1;

                final MutableColumn idColumn = new MutableColumn(HBaseDataContext.FIELD_ID, _defaultRowKeyColumnType)
                        .setPrimaryKey(true)
                        .setColumnNumber(columnNumber)
                        .setTable(this);
                addColumn(idColumn);
                columnNumber++;

                // What about timestamp?

                final HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
                for (int i = 0; i < columnFamilies.length; i++) {
                    final HColumnDescriptor columnDescriptor = columnFamilies[i];
                    final String columnFamilyName = columnDescriptor.getNameAsString();
                    // HBase column families are always unstructured maps.
                    final ColumnType type = ColumnType.MAP;
                    final MutableColumn column = new MutableColumn(columnFamilyName, type);
                    column.setTable(this);
                    column.setColumnNumber(columnNumber);
                    columnNumber++;
                    addColumn(column);
                }
            } catch (Exception e) {
                throw new MetaModelException("Could not resolve table ", e);
            }
        }
        return columnsInternal;
    }

    /**
     * Check if a list of columnNames all exist in this table
     * If a column doesn't exist, then a {@link MetaModelException} is thrown
     * @param columnNamesOfCheckedTable
     */
    public void checkForNotMatchingColumns(final List<String> columnNamesOfCheckedTable) {
        final List<String> columnsNamesOfExistingTable = getColumnNames();
        for (String columnNameOfCheckedTable : columnNamesOfCheckedTable) {
            boolean matchingColumnFound = false;
            int i = 0;
            while (!matchingColumnFound && i < columnsNamesOfExistingTable.size()) {
                if (columnNameOfCheckedTable.equals(columnsNamesOfExistingTable.get(i))) {
                    matchingColumnFound = true;
                } else {
                    i++;
                }
            }
            if (!matchingColumnFound) {
                throw new MetaModelException(String.format("ColumnFamily: %s doesn't exist in the schema of the table",
                        columnNameOfCheckedTable));
            }
        }
    }
}
