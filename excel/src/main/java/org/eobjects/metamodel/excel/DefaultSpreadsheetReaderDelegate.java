/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.excel;

import java.io.InputStream;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.EmptyDataSet;
import org.eobjects.metamodel.data.MaxRowsDataSet;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.AlphabeticSequence;
import org.eobjects.metamodel.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default {@link SpreadsheetReaderDelegate}, which uses POI's main user
 * model to read spreadsheets: the Workbook class.
 * 
 * @author Kasper Sørensen
 */
final class DefaultSpreadsheetReaderDelegate implements SpreadsheetReaderDelegate {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSpreadsheetReaderDelegate.class);

    private final ExcelConfiguration _configuration;

    public DefaultSpreadsheetReaderDelegate(ExcelConfiguration configuration) {
        _configuration = configuration;
    }

    @Override
    public Schema createSchema(InputStream inputStream, String schemaName) {
        final MutableSchema schema = new MutableSchema(schemaName);
        final Workbook wb = ExcelUtils.readWorkbook(inputStream);

        for (int i = 0; i < wb.getNumberOfSheets(); i++) {
            final Sheet currentSheet = wb.getSheetAt(i);
            final MutableTable table = createTable(wb, currentSheet);
            table.setSchema(schema);
            schema.addTable(table);
        }

        return schema;
    }

    @Override
    public DataSet executeQuery(InputStream inputStream, Table table, Column[] columns, int maxRows) {
        final Workbook wb = ExcelUtils.readWorkbook(inputStream);
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
    public void notifyTablesModified(Ref<InputStream> inputStreamRef) {
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

            // build columns by using alphabetic sequences
            // (A,B,C...)
            AlphabeticSequence sequence = new AlphabeticSequence();

            final int offset = getColumnOffset(row);
            for (int i = 0; i < offset; i++) {
                sequence.next();
            }

            for (int j = offset; j < row.getLastCellNum(); j++) {
                Column column = new MutableColumn(sequence.next(), ColumnType.VARCHAR, table, j, true);
                table.addColumn(column);
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
        for (int j = offset; j < rowLength; j++) {
            Cell cell = row.getCell(j);
            String columnName = ExcelUtils.getCellValue(wb, cell);
            if (columnName == null || "".equals(columnName)) {
                columnName = "[Column " + (j + 1) + "]";
            }
            Column column = new MutableColumn(columnName, ColumnType.VARCHAR, table, j, true);
            table.addColumn(column);
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
