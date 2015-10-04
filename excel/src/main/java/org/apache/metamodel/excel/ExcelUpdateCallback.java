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

import org.apache.metamodel.AbstractUpdateCallback;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Style.Color;
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.drop.TableDropBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.poi.hssf.usermodel.HSSFPalette;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

final class ExcelUpdateCallback extends AbstractUpdateCallback implements UpdateCallback {

    private final ExcelDataContext _dataContext;
    private final ExcelConfiguration _configuration;
    private boolean _sheetsModified;
    private Workbook _workbook;
    private Short _dateCellFormat;
    private CellStyle _dateCellStyle;

    public ExcelUpdateCallback(ExcelDataContext dataContext) {
        super(dataContext);
        _sheetsModified = false;
        _configuration = dataContext.getConfiguration();
        _dataContext = dataContext;
    }

    @Override
    public TableCreationBuilder createTable(Schema schema, String name)
            throws IllegalArgumentException, IllegalStateException {
        return new ExcelTableCreationBuilder(this, schema, name);
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException {
        return new ExcelInsertBuilder(this, table);
    }

    protected ExcelConfiguration getConfiguration() {
        return _configuration;
    }

    @Override
    public ExcelDataContext getDataContext() {
        return _dataContext;
    }

    protected void close() {
        if (_workbook != null) {
            ExcelUtils.writeAndCloseWorkbook(_dataContext, _workbook);

            _workbook = null;
            _dateCellFormat = null;
            _dateCellStyle = null;
        }
        if (_sheetsModified) {
            _dataContext.notifyTablesModified();
            _sheetsModified = false;
        }
    }

    protected Workbook getWorkbook(boolean streamingAllowed) {
        if (_workbook == null || (!streamingAllowed && _workbook instanceof SXSSFWorkbook)) {
            if (_workbook != null) {
                ExcelUtils.writeAndCloseWorkbook(_dataContext, _workbook);
            }
            _workbook = ExcelUtils.readWorkbook(_dataContext);
            if (streamingAllowed && _workbook instanceof XSSFWorkbook) {
                _workbook = new SXSSFWorkbook((XSSFWorkbook) _workbook);
            }
        }
        return _workbook;
    }

    protected Sheet createSheet(String name) {
        Sheet sheet = getWorkbook(true).createSheet(name);
        _sheetsModified = true;
        return sheet;
    }

    protected void removeSheet(String name) {
        int index = getWorkbook(true).getSheetIndex(name);
        if (index != -1) {
            getWorkbook(true).removeSheetAt(index);
            _sheetsModified = true;
        }
    }

    protected Row createRow(String name) {
        if (_sheetsModified) {
            close();
        }
        Sheet sheet = getWorkbook(true).getSheet(name);
        int lastRowNum = getLastRowNum(sheet);
        Row row = sheet.createRow(lastRowNum + 1);
        return row;
    }

    private int getLastRowNum(Sheet sheet) {
        final int lastRowNum = sheet.getLastRowNum();
        if (lastRowNum == 0 && sheet instanceof SXSSFSheet) {
            // streaming sheets have bad behaviour in this scenario - since no
            // rows are in cache, it will return 0!
            DataSet ds = _dataContext.query().from(sheet.getSheetName()).selectCount().execute();
            ds.next();
            final Number count = (Number) ds.getRow().getValue(0);
            final int columnNameLineNumber = _configuration.getColumnNameLineNumber();
            int oneBasedResult = count.intValue()
                    + (columnNameLineNumber == ExcelConfiguration.NO_COLUMN_NAME_LINE ? 0 : columnNameLineNumber);
            return oneBasedResult - 1;
        }
        return lastRowNum;
    }

    /**
     * Creates a new cell style in the spreadsheet
     * 
     * @return
     */
    public CellStyle createCellStyle() {
        Workbook workbook = getWorkbook(true);
        return workbook.createCellStyle();
    }

    public Font createFont() {
        Workbook workbook = getWorkbook(true);
        return workbook.createFont();
    }

    protected Sheet getSheet(String name) {
        return getWorkbook(true).getSheet(name);
    }

    /**
     * Gets the index identifier for the date format
     * 
     * @return
     */
    public short getDateCellFormat() {
        if (_dateCellFormat == null) {
            Workbook workbook = getWorkbook(true);
            _dateCellFormat = workbook.getCreationHelper().createDataFormat().getFormat("m/d/yy h:mm");
        }
        return _dateCellFormat;
    }

    /**
     * Gets a shared, reusable cell style for "pure date" cells (eg. no other
     * styling applied)
     * 
     * @return
     */
    public CellStyle getDateCellStyle() {
        if (_dateCellStyle == null) {
            _dateCellStyle = createCellStyle();
            _dateCellStyle.setDataFormat(getDateCellFormat());
        }
        return _dateCellStyle;
    }

    public short getColorIndex(Color color) {
        Workbook workbook = getWorkbook(true);
        if (workbook instanceof HSSFWorkbook) {
            HSSFPalette palette = ((HSSFWorkbook) workbook).getCustomPalette();
            byte r = toRgb(color.getRed());
            byte g = toRgb(color.getGreen());
            byte b = toRgb(color.getBlue());

            HSSFColor index = palette.findColor(r, g, b);
            if (index == null) {
                index = palette.findSimilarColor(r, g, b);
            }
            return index.getIndex();
        }
        throw new IllegalStateException("Unexpected workbook type: " + workbook.getClass());
    }

    private byte toRgb(int i) {
        assert i >= 0;
        assert i <= 255;

        if (i > 127) {
            i = i - 256;
        }
        return (byte) i;
    }

    @Override
    public boolean isDropTableSupported() {
        return true;
    }

    @Override
    public TableDropBuilder dropTable(Table table) throws UnsupportedOperationException {
        return new ExcelDropTableBuilder(this, table);
    }

    @Override
    public boolean isDeleteSupported() {
        return true;
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) throws UnsupportedOperationException {
        return new ExcelDeleteBuilder(this, table);
    }
}
