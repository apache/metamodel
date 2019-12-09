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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.EmptyDataSet;
import org.apache.metamodel.data.MaxRowsDataSet;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.schema.naming.ColumnNamingContext;
import org.apache.metamodel.schema.naming.ColumnNamingContextImpl;
import org.apache.metamodel.schema.naming.ColumnNamingSession;
import org.apache.metamodel.schema.naming.ColumnNamingStrategy;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.Resource;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
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

    static final ColumnType DEFAULT_COLUMN_TYPE = ColumnType.STRING;
    static final ColumnType LEGACY_COLUMN_TYPE = ColumnType.VARCHAR;

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
        final Workbook wb = ExcelUtils.readWorkbook(_resource, true);
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
    public DataSet executeQuery(Table table, List<Column> columns, int maxRows) {
        final Workbook wb = ExcelUtils.readWorkbook(_resource, true);
        final Sheet sheet = wb.getSheet(table.getName());

        if (sheet == null || sheet.getPhysicalNumberOfRows() == 0) {
            return new EmptyDataSet(columns.stream().map(SelectItem::new).collect(Collectors.toList()));
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
        final MutableTable table = new MutableTable(sheet.getSheetName(), TableType.TABLE);

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

        final ColumnType[] columnTypes = getColumnTypes(sheet);
        if (columnTypes == null) {
            return table;
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

                for (int i = offset; i < row.getLastCellNum(); i++) {
                    final ColumnNamingContext namingContext = new ColumnNamingContextImpl(table, null, i);
                    final Column column = new MutableColumn(columnNamingSession.getNextColumnName(namingContext),
                            columnTypes[i], table, i, true);
                    table.addColumn(column);
                }
            }

        } else {
            row = iterateToColumnNameRow(rowIterator, row);

            if (row != null) {
                createColumns(table, wb, row, columnTypes);
            }
        }

        return table;
    }

    /**
     * Iterate to the column name row if the configured ColumnNameLineNumber is above 1.
     * @param rowIterator
     * @param currentRow
     * @return Returns the column name row. Returns the current row if the configured ColumnNameLineNumber is 1.
     * Returns null if the columnName row is not found.
     */
    private Row iterateToColumnNameRow(final Iterator<Row> rowIterator, final Row currentRow) {
        Row row = currentRow;

        // iterate to the column name line number (if above 1)
        for (int i = 1; i < _configuration.getColumnNameLineNumber(); i++) {
            if (rowIterator.hasNext()) {
                row = rowIterator.next();
            } else {
                return null;
            }
        }

        return row;
    }

    /**
     * Get an array of {@link ColumnType}s. The length of the array is determined by the header row. If there's no
     * configured column name line, then the first data row is used. If the {@link ColumnType} should be detected, then
     * this is done by using the data rows only. If this shouldn't be detected, then the array is filled with either
     * default column type when there is no column name line or legacy column type when there is a column name line.
     * @param sheet
     * @return
     */
    private ColumnType[] getColumnTypes(final Sheet sheet) {
        // To find the array length we need the header
        final Iterator<Row> iterator = ExcelUtils.getRowIterator(sheet, _configuration, false);
        Row row;
        if (_configuration.getColumnNameLineNumber() == ExcelConfiguration.NO_COLUMN_NAME_LINE) {
            row = findTheFirstNonEmptyRow(iterator);
        } else {
            row = iterateToColumnNameRow(iterator, iterator.next());
        }
        if (row == null) {
            return null;
        }

        final ColumnType[] columnTypes = new ColumnType[row.getLastCellNum()];

        if (_configuration.isDetectColumnTypes()) {
            // Now we need the first data row
            row = findTheFirstNonEmptyRow(iterator);
            if (row != null) {
                new ColumnTypeScanner(sheet).detectColumnTypes(row, iterator, columnTypes);
            }
        } else {
            if (_configuration.getColumnNameLineNumber() == ExcelConfiguration.NO_COLUMN_NAME_LINE) {
                Arrays.fill(columnTypes, DEFAULT_COLUMN_TYPE);
            } else {
                Arrays.fill(columnTypes, LEGACY_COLUMN_TYPE);
            }
        }
        return columnTypes;
    }

    private static Row findTheFirstNonEmptyRow(final Iterator<Row> rowIterator) {
        while (rowIterator.hasNext()) {
            final Row row = rowIterator.next();
            if (row != null) {
                return row;
            }
        }
        return null;
    }

    /**
     * Builds columns based on row/cell values.
     * 
     * @param table
     * @param wb
     * @param row
     */
    private void createColumns(final MutableTable table, final Workbook wb, final Row row,
            final ColumnType[] columnTypes) {
        if (row == null) {
            logger.warn("Cannot create columns based on null row!");
            return;
        }
        final short rowLength = row.getLastCellNum();

        final int offset = getColumnOffset(row);

        // build columns based on cell values.
        try (final ColumnNamingSession columnNamingSession = _configuration
                .getColumnNamingStrategy()
                .startColumnNamingSession()) {
            for (int i = offset; i < rowLength; i++) {
                final Cell cell = row.getCell(i);
                final String intrinsicColumnName = ExcelUtils.getCellValue(wb, cell);
                final ColumnNamingContext columnNamingContext = new ColumnNamingContextImpl(table, intrinsicColumnName,
                        i);
                final String columnName = columnNamingSession.getNextColumnName(columnNamingContext);
                final Column column = new MutableColumn(columnName, columnTypes[i], table, i, true);
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
    
    private class ColumnTypeScanner {
        final FormulaEvaluator formulaEvaluator;

        ColumnTypeScanner(final Sheet sheet) {
            formulaEvaluator = sheet.getWorkbook().getCreationHelper().createFormulaEvaluator();
        }

        private void detectColumnTypes(final Row firstRow, final Iterator<Row> dataRowIterator,
                final ColumnType[] columnTypes) {
            detectColumnTypesFirstRow(firstRow, columnTypes);
            detectColumnTypesOtherRows(dataRowIterator, columnTypes);

            // If all cells are null, then this loop sets the column type to the default
            for (int i = 0; i < columnTypes.length; i++) {
                if (columnTypes[i] == null) {
                    columnTypes[i] = DEFAULT_COLUMN_TYPE;
                }
            }
        }

        private void detectColumnTypesFirstRow(final Row firstRow, final ColumnType[] columnTypes) {
            if (firstRow != null && firstRow.getLastCellNum() > 0) {
                for (int i = getColumnOffset(firstRow); i < columnTypes.length; i++) {
                    if (firstRow.getCell(i) != null) {
                        columnTypes[i] = determineColumnTypeFromCell(firstRow.getCell(i));
                    }
                }
            }
        }

        private void detectColumnTypesOtherRows(final Iterator<Row> dataRowIterator, final ColumnType[] columnTypes) {
            int numberOfLinesToScan = _configuration.getNumberOfLinesToScan() - 1;

            while (dataRowIterator.hasNext() && numberOfLinesToScan-- > 0) {
                final Row currentRow = dataRowIterator.next();
                if (currentRow != null && currentRow.getLastCellNum() > 0) {
                    for (int i = getColumnOffset(currentRow); i < columnTypes.length; i++) {
                        final ColumnType detectNewColumnType = detectNewColumnTypeCell(columnTypes[i], currentRow
                                .getCell(i));
                        if (detectNewColumnType != null) {
                            columnTypes[i] = detectNewColumnType;
                        }
                    }
                }
            }
        }

        /**
         * Tries to detect a new {@link ColumnType} for a cell.
         * @param currentColumnType
         * @param cell
         * @return Returns a new {@link ColumnType} when detected. Otherwise null is returned.
         */
        private ColumnType detectNewColumnTypeCell(final ColumnType currentColumnType, final Cell cell) {
            // Can't detect something new if it's already on the default.
            if (currentColumnType != null && currentColumnType.equals(DEFAULT_COLUMN_TYPE)) {
                return null;
            }
            // Skip if the cell is null. This way 1 missing cell can't influence the column type of all other cells.
            if (cell == null) {
                return null;
            }

            final ColumnType detectedColumnType = determineColumnTypeFromCell(cell);
            if (currentColumnType == null) {
                return detectedColumnType;
            } else if (!currentColumnType.equals(detectedColumnType)) {
                // If the column type is Double and a Integer is detected, then don't set it to Integer
                if (currentColumnType.equals(ColumnType.INTEGER) && detectedColumnType.equals(ColumnType.DOUBLE)) {
                    // If the column type is Integer and a Double is detected, then set it to Double
                    return detectedColumnType;
                } else if (currentColumnType.equals(ColumnType.DOUBLE) && detectedColumnType
                        .equals(ColumnType.INTEGER)) {
                    return null;
                } else {
                    return DEFAULT_COLUMN_TYPE;
                }
            }
            return null;
        }

        private ColumnType determineColumnTypeFromCell(final Cell cell) {
            switch (cell.getCellType()) {
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return ColumnType.DATE;
                } else {
                    return cell.getNumericCellValue() % 1 == 0 ? ColumnType.INTEGER : ColumnType.DOUBLE;
                }
            case BOOLEAN:
                return ColumnType.BOOLEAN;
            case FORMULA:
                return determineColumnTypeFromCell(formulaEvaluator.evaluateInCell(cell));
            case STRING:
                // fall through
            case BLANK:
                // fall through
            case _NONE:
                // fall through
            case ERROR:
                // fall through
            default:
                return DEFAULT_COLUMN_TYPE;
            }
        }
    }
}
