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
package org.apache.metamodel.excel;

import java.util.Iterator;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.EmptyDataSet;
import org.apache.metamodel.data.MaxRowsDataSet;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.naming.ColumnNamingContext;
import org.apache.metamodel.schema.naming.ColumnNamingContextImpl;
import org.apache.metamodel.schema.naming.ColumnNamingSession;
import org.apache.metamodel.schema.naming.ColumnNamingStrategy;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.Resource;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default {@link SpreadsheetReaderDelegate}, which uses POI's main user
 * model to read spreadsheets: the Workbook class.
 */
final class DefaultSpreadsheetReaderDelegate implements SpreadsheetReaderDelegate {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSpreadsheetReaderDelegate.class);

    private final Resource _resource;
    private final ExcelConfiguration _configuration;

    public DefaultSpreadsheetReaderDelegate(Resource resource, ExcelConfiguration configuration) {
        _resource = resource;
        _configuration = configuration;
    }

    @Override
    public Schema createSchema(String schemaName) {
        final MutableSchema schema = new MutableSchema(schemaName);
        final Workbook wb = ExcelUtils.readWorkbook(_resource);
        try {
            for (int i = 0; i < wb.getNumberOfSheets(); i++) {
                final Sheet currentSheet = wb.getSheetAt(i);
                final MutableTable table = createTable(wb, currentSheet);
                table.setSchema(schema);
                schema.addTable(table);
            }

            return schema;
        } finally {
            FileHelper.safeClose(wb);
        }
    }

    @Override
    public DataSet executeQuery(Table table, Column[] columns, int maxRows) {
        final Workbook wb = ExcelUtils.readWorkbook(_resource);
        final Sheet sheet = wb.getSheet(table.getName());

        if (sheet == null || sheet.getPhysicalNumberOfRows() == 0) {
            return new EmptyDataSet(columns);
        }

        DataSet dataSet = ExcelUtils.getDataSet(wb, sheet, table, _configuration);

        if (maxRows > 0) {
            dataSet = new MaxRowsDataSet(dataSet, maxRows);
        }
        return dataSet;
    }

    @Override
    public void notifyTablesModified() {
        // do nothing
    }

    private MutableTable createTable(final Workbook wb, final Sheet sheet) {
        final MutableTable table = new MutableTable(sheet.getSheetName());

        if (sheet.getPhysicalNumberOfRows() <= 0) {
            // no physical rows in sheet
            return table;
        }

        final Iterator<Row> rowIterator = ExcelUtils.getRowIterator(sheet, _configuration, false);

        if (!rowIterator.hasNext()) {
            // no physical rows in sheet
            return table;
        }

        Row row = null;

        if (_configuration.isSkipEmptyLines()) {
            while (row == null && rowIterator.hasNext()) {
                row = rowIterator.next();
            }
        } else {
            row = rowIterator.next();
        }

        final int columnNameLineNumber = _configuration.getColumnNameLineNumber();
        if (columnNameLineNumber == ExcelConfiguration.NO_COLUMN_NAME_LINE) {

            // get to the first non-empty line (no matter if lines are skipped
            // or not we need to read ahead to figure out how many columns there
            // are!)
            while (row == null && rowIterator.hasNext()) {
                row = rowIterator.next();
            }

            // build columns without any intrinsic column names
            final ColumnNamingStrategy columnNamingStrategy = _configuration.getColumnNamingStrategy();
            try (final ColumnNamingSession columnNamingSession = columnNamingStrategy.startColumnNamingSession()) {
                final int offset = getColumnOffset(row);
                for (int i = 0; i < offset; i++) {
                    columnNamingSession.getNextColumnName(new ColumnNamingContextImpl(i));
                }

                for (int j = offset; j < row.getLastCellNum(); j++) {
                    final ColumnNamingContext namingContext = new ColumnNamingContextImpl(table, null, j);
                    final Column column = new MutableColumn(columnNamingSession.getNextColumnName(namingContext),
                            ColumnType.STRING, table, j, true);
                    table.addColumn(column);
                }
            }

        } else {

            boolean hasColumns = true;

            // iterate to the column name line number (if above 1)
            for (int j = 1; j < columnNameLineNumber; j++) {
                if (rowIterator.hasNext()) {
                    row = rowIterator.next();
                } else {
                    hasColumns = false;
                    break;
                }
            }

            if (hasColumns) {
                createColumns(table, wb, row);
            }
        }

        return table;
    }

    /**
     * Builds columns based on row/cell values.
     * 
     * @param table
     * @param wb
     * @param row
     */
    private void createColumns(MutableTable table, Workbook wb, Row row) {
        if (row == null) {
            logger.warn("Cannot create columns based on null row!");
            return;
        }
        final short rowLength = row.getLastCellNum();

        final int offset = getColumnOffset(row);

        // build columns based on cell values.
        try (final ColumnNamingSession columnNamingSession = _configuration.getColumnNamingStrategy()
                .startColumnNamingSession()) {
            for (int j = offset; j < rowLength; j++) {
                final Cell cell = row.getCell(j);
                final String intrinsicColumnName = ExcelUtils.getCellValue(wb, cell);
                final ColumnNamingContext columnNamingContext = new ColumnNamingContextImpl(table, intrinsicColumnName,
                        j);
                final String columnName = columnNamingSession.getNextColumnName(columnNamingContext);
                final Column column = new MutableColumn(columnName, ColumnType.VARCHAR, table, j, true);
                table.addColumn(column);
            }
        }
    }

    /**
     * Gets the column offset (first column to include). This is dependent on
     * the row used for column processing and whether the skip empty columns
     * property is set.
     * 
     * @param row
     * @return
     */
    private int getColumnOffset(Row row) {
        final int offset;
        if (_configuration.isSkipEmptyColumns()) {
            offset = row.getFirstCellNum();
        } else {
            offset = 0;
        }
        return offset;
    }
}
