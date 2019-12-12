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

import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.EmptyDataSet;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.data.Style.SizeUnit;
import org.apache.metamodel.data.StyleBuilder;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.DateUtils;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.InMemoryResource;
import org.apache.metamodel.util.Resource;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Color;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.FontUnderline;
import org.apache.poi.ss.usermodel.FormulaError;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.XMLReader;

/**
 * Convenience/reusable methods for Excel workbook handling.
 */
final class ExcelUtils {

    private static final Logger logger = LoggerFactory.getLogger(ExcelUtils.class);

    private ExcelUtils() {
        // prevent instantiation
    }

    public static XMLReader createXmlReader() {
        try {
            SAXParserFactory saxFactory = SAXParserFactory.newInstance();
            SAXParser saxParser = saxFactory.newSAXParser();
            XMLReader sheetParser = saxParser.getXMLReader();
            return sheetParser;
        } catch (Exception e) {
            throw new MetaModelException(e);
        }
    }

    /**
     * Opens a {@link Workbook} based on a {@link Resource}.
     * 
     * @param resource
     * @param allowFileOptimization whether or not to allow POI to use file handles which supposedly speeds things up,
     *            but creates issues when writing multiple times to the same file in short bursts of time.
     * @return
     */
    public static Workbook readWorkbook(Resource resource, boolean allowFileOptimization) {
        if (!resource.isExists()) {
            // resource does not exist- create a blank workbook
            if (isXlsxFile(resource)) {
                return new SXSSFWorkbook(1000);
            } else {
                return new HSSFWorkbook();
            }
        }

        if (allowFileOptimization && resource instanceof FileResource) {
            final File file = ((FileResource) resource).getFile();
            try {
                // open read-only mode
                return WorkbookFactory.create(file, null, true);
            } catch (Exception e) {
                logger.error("Could not open workbook", e);
                throw new IllegalStateException("Could not open workbook", e);
            }
        }

        return resource.read(inputStream -> {
            try {
                return WorkbookFactory.create(inputStream);
            } catch (Exception e) {
                logger.error("Could not open workbook", e);
                throw new IllegalStateException("Could not open workbook", e);
            }
        });
    }

    public static boolean isXlsxFile(Resource resource) {
        if (resource == null) {
            return false;
        }
        return resource.getName().toLowerCase().endsWith(".xlsx");
    }

    /**
     * Initializes a workbook instance based on a {@link ExcelDataContext}.
     * 
     * @return a workbook instance based on the ExcelDataContext.
     */
    public static Workbook readWorkbookForUpdate(ExcelDataContext dataContext) {
        Resource resource = dataContext.getResource();
        return readWorkbook(resource, false);
    }

    /**
     * Writes the {@link Workbook} to a {@link Resource}. The {@link Workbook} will be closed as a result of this
     * operation!
     * 
     * @param dataContext
     * @param wb
     */
    public static void writeAndCloseWorkbook(ExcelDataContext dataContext, final Workbook wb) {
        // first write to a temp file to avoid that workbook source is the same
        // as the target (will cause read+write cyclic overflow)

        final Resource realResource = dataContext.getResource();
        final Resource tempResource = new InMemoryResource(realResource.getQualifiedPath());

        tempResource.write(out -> wb.write(out));

        FileHelper.safeClose(wb);

        FileHelper.copy(tempResource, realResource);

    }

    public static String getCellValue(Workbook wb, Cell cell) {
        if (cell == null) {
            return null;
        }

        final String result;

        switch (cell.getCellType()) {
        case BLANK:
        case _NONE:
            result = null;
            break;
        case BOOLEAN:
            result = Boolean.toString(cell.getBooleanCellValue());
            break;
        case ERROR:
            result = getErrorResult(cell);
            break;
        case FORMULA:
            result = getFormulaCellValue(wb, cell);
            break;
        case NUMERIC:
            if (DateUtil.isCellDateFormatted(cell)) {
                Date date = cell.getDateCellValue();
                if (date == null) {
                    result = null;
                } else {
                    result = DateUtils.createDateFormat().format(date);
                }
            } else {
                result = getNumericCellValueAsString(cell.getCellStyle(), cell.getNumericCellValue());
            }
            break;
        case STRING:
            result = cell.getRichStringCellValue().getString();
            break;
        default:
            throw new IllegalStateException("Unknown cell type: " + cell.getCellType());
        }

        logger.debug("cell ({},{}) resolved to value: {}", cell.getRowIndex(), cell.getColumnIndex(), result);

        return result;
    }

    private static Object getCellValueAsObject(final Workbook workbook, final Cell cell) {
        if (cell == null) {
            return null;
        }

        final Object result;

        switch (cell.getCellType()) {
        case BLANK:
        case _NONE:
            result = null;
            break;
        case BOOLEAN:
            result = Boolean.valueOf(cell.getBooleanCellValue());
            break;
        case ERROR:
            result = getErrorResult(cell);
            break;
        case FORMULA:
            result = getFormulaCellValueAsObject(workbook, cell);
            break;
        case NUMERIC:
            if (DateUtil.isCellDateFormatted(cell)) {
                result = cell.getDateCellValue();
            } else {
                result = getDoubleAsNumber(cell.getNumericCellValue());
            }
            break;
        case STRING:
            result = cell.getRichStringCellValue().getString();
            break;
        default:
            throw new IllegalStateException("Unknown cell type: " + cell.getCellType());
        }

        logger.debug("cell ({},{}) resolved to value: {}", cell.getRowIndex(), cell.getColumnIndex(), result);

        return result;
    }

    private static String getErrorResult(final Cell cell) {
        try {
            return FormulaError.forInt(cell.getErrorCellValue()).getString();
        } catch (final RuntimeException e) {
            logger
                    .debug("Getting error code for ({},{}) failed!: {}", cell.getRowIndex(), cell.getColumnIndex(), e
                            .getMessage());
            if (cell instanceof XSSFCell) {
                // hack to get error string, which is available
                return ((XSSFCell) cell).getErrorCellString();
            } else {
                logger
                        .error("Couldn't handle unexpected error scenario in cell: ({},{})", cell.getRowIndex(), cell
                                .getColumnIndex());
                throw e;
            }
        }
    }

    private static Object evaluateCell(final Workbook workbook, final Cell cell, final ColumnType expectedColumnType) {
        final Object value = getCellValueAsObject(workbook, cell);
        if (value == null || value.getClass().equals(expectedColumnType.getJavaEquivalentClass()) || (value
                .getClass()
                .equals(Integer.class) && expectedColumnType.getJavaEquivalentClass().equals(Double.class))) {
            return value;
        } else {
            if (logger.isWarnEnabled()) {
                logger
                        .warn("Cell ({},{}) has the value '{}' of data type '{}', which doesn't match the detected "
                                + "column's data type '{}'. This cell gets value NULL in the DataSet.", cell
                                        .getRowIndex(), cell.getColumnIndex(), value, value.getClass().getSimpleName(),
                                expectedColumnType);
            }
            return null;
        }
    }

    private static String getFormulaCellValue(Workbook workbook, Cell cell) {
        // first try with a cached/precalculated value
        try {
            double numericCellValue = cell.getNumericCellValue();
            return getNumericCellValueAsString(cell.getCellStyle(), numericCellValue);
        } catch (Exception e) {
            if (logger.isInfoEnabled()) {
                logger.info("Failed to fetch cached/precalculated formula value of cell: " + cell, e);
            }
        }

        // evaluate cell first, if possible
        final Cell evaluatedCell = getEvaluatedCell(workbook, cell);
        if (evaluatedCell != null) {
            return getCellValue(workbook, evaluatedCell);
        } else {
            // last resort: return the string formula
            return cell.getCellFormula();
        }
    }

    private static Object getFormulaCellValueAsObject(final Workbook workbook, final Cell cell) {
        // first try with a cached/precalculated value
        try {
            return getDoubleAsNumber(cell.getNumericCellValue());
        } catch (final Exception e) {
            if (logger.isInfoEnabled()) {
                logger.info("Failed to fetch cached/precalculated formula value of cell: " + cell, e);
            }
        }

        // evaluate cell first, if possible
        final Cell evaluatedCell = getEvaluatedCell(workbook, cell);
        if (evaluatedCell != null) {
            return getCellValueAsObject(workbook, evaluatedCell);
        } else {
            // last resort: return the string formula
            return cell.getCellFormula();
        }
    }

    private static Cell getEvaluatedCell(final Workbook workbook, final Cell cell) {
        try {
            if (logger.isInfoEnabled()) {
                logger
                        .info("cell ({},{}) is a formula. Attempting to evaluate: {}", cell.getRowIndex(), cell
                                .getColumnIndex(), cell.getCellFormula());
            }

            final FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();

            // calculates the formula and puts it's value back into the cell
            return evaluator.evaluateInCell(cell);
        } catch (RuntimeException e) {
            logger
                    .warn("Exception occurred while evaluating formula at position ({},{}): {}", cell.getRowIndex(),
                            cell.getColumnIndex(), e.getMessage());
        }
        return null;
    }

    private static Number getDoubleAsNumber(final double value) {
        final Double doubleValue = Double.valueOf(value);
        if (doubleValue % 1 == 0 && doubleValue <= Integer.MAX_VALUE) {
            return Integer.valueOf(doubleValue.intValue());
        } else {
            return doubleValue;
        }
    }

    public static Style getCellStyle(Workbook workbook, Cell cell) {
        if (cell == null) {
            return Style.NO_STYLE;
        }
        final CellStyle cellStyle = cell.getCellStyle();

        final int fontIndex = cellStyle.getFontIndexAsInt();
        final Font font = workbook.getFontAt(fontIndex);
        final StyleBuilder styleBuilder = new StyleBuilder();

        // Font bold, italic, underline
        if (font.getBold()) {
            styleBuilder.bold();
        }
        if (font.getItalic()) {
            styleBuilder.italic();
        }
        if (font.getUnderline() != FontUnderline.NONE.getByteValue()) {
            styleBuilder.underline();
        }

        // Font size
        final Font stdFont = workbook.getFontAt(0);
        final short fontSize = font.getFontHeightInPoints();
        if (stdFont.getFontHeightInPoints() != fontSize) {
            styleBuilder.fontSize(fontSize, SizeUnit.PT);
        }

        // Font color
        final short colorIndex = font.getColor();
        if (font instanceof HSSFFont) {
            if (colorIndex != HSSFFont.COLOR_NORMAL) {
                final HSSFWorkbook wb = (HSSFWorkbook) workbook;
                HSSFColor color = wb.getCustomPalette().getColor(colorIndex);
                if (color != null) {
                    short[] triplet = color.getTriplet();
                    styleBuilder.foreground(triplet);
                }
            }
        } else if (font instanceof XSSFFont) {
            XSSFFont xssfFont = (XSSFFont) font;

            XSSFColor color = xssfFont.getXSSFColor();
            if (color != null) {
                String argbHex = color.getARGBHex();
                if (argbHex != null) {
                    styleBuilder.foreground(argbHex.substring(2));
                }
            }
        } else {
            throw new IllegalStateException("Unexpected font type: " + (font == null ? "null" : font.getClass()) + ")");
        }

        // Background color
        if (cellStyle.getFillPattern() == FillPatternType.SOLID_FOREGROUND) {
            Color color = cellStyle.getFillForegroundColorColor();
            if (color instanceof HSSFColor) {
                short[] triplet = ((HSSFColor) color).getTriplet();
                if (triplet != null) {
                    styleBuilder.background(triplet);
                }
            } else if (color instanceof XSSFColor) {
                String argb = ((XSSFColor) color).getARGBHex();
                if (argb != null) {
                    styleBuilder.background(argb.substring(2));
                }
            } else {
                throw new IllegalStateException(
                        "Unexpected color type: " + (color == null ? "null" : color.getClass()) + ")");
            }
        }

        // alignment
        switch (cellStyle.getAlignment()) {
        case LEFT:
            styleBuilder.leftAligned();
            break;
        case RIGHT:
            styleBuilder.rightAligned();
            break;
        case CENTER:
            styleBuilder.centerAligned();
            break;
        case JUSTIFY:
            styleBuilder.justifyAligned();
            break;
        default:
            // we currently don't support other alignment styles
            break;
        }

        return styleBuilder.create();
    }

    public static Iterator<Row> getRowIterator(Sheet sheet, ExcelConfiguration configuration, boolean jumpToDataRows) {
        final Iterator<Row> iterator;
        if (configuration.isSkipEmptyLines()) {
            iterator = sheet.rowIterator();
        } else {
            iterator = new ZeroBasedRowIterator(sheet);
        }

        if (jumpToDataRows) {
            final int columnNameLineNumber = configuration.getColumnNameLineNumber();
            if (columnNameLineNumber != ExcelConfiguration.NO_COLUMN_NAME_LINE) {
                // iterate past the column headers
                if (iterator.hasNext()) {
                    iterator.next();
                }
                for (int i = 1; i < columnNameLineNumber; i++) {
                    if (iterator.hasNext()) {
                        iterator.next();
                    } else {
                        // no more rows!
                        break;
                    }
                }
            }
        }

        return iterator;
    }

    /**
     * Creates a MetaModel row based on an Excel row
     * 
     * @param workbook
     * @param row
     * @param selectItems select items of the columns in the table
     * @return
     */
    public static DefaultRow createRow(final Workbook workbook, final Row row, final DataSetHeader header) {
        final int size = header.size();
        final Object[] values = new Object[size];
        final Style[] styles = new Style[size];
        if (row != null) {
            for (int i = 0; i < size; i++) {
                final int columnNumber = header.getSelectItem(i).getColumn().getColumnNumber();
                final ColumnType columnType = header.getSelectItem(i).getColumn().getType();
                final Cell cell = row.getCell(columnNumber);
                final Object value;
                if (columnType.equals(DefaultSpreadsheetReaderDelegate.DEFAULT_COLUMN_TYPE) || columnType
                        .equals(DefaultSpreadsheetReaderDelegate.LEGACY_COLUMN_TYPE)) {
                    value = ExcelUtils.getCellValue(workbook, cell);
                } else {
                    value = ExcelUtils.evaluateCell(workbook, cell, columnType);
                }

                final Style style = ExcelUtils.getCellStyle(workbook, cell);
                values[i] = value;
                styles[i] = style;
            }
        }

        return new DefaultRow(header, values, styles);
    }

    public static DataSet getDataSet(Workbook workbook, Sheet sheet, Table table, ExcelConfiguration configuration) {
        final List<SelectItem> selectItems =
                table.getColumns().stream().map(SelectItem::new).collect(Collectors.toList());
        final Iterator<Row> rowIterator = getRowIterator(sheet, configuration, true);
        if (!rowIterator.hasNext()) {
            // no more rows!
            FileHelper.safeClose(workbook);
            return new EmptyDataSet(selectItems);
        }

        final DataSet dataSet = new XlsDataSet(selectItems, workbook, rowIterator);
        return dataSet;
    }

    private static String getNumericCellValueAsString(final CellStyle cellStyle, final double cellValue) {
        final int formatIndex = cellStyle.getDataFormat();
        String formatString = cellStyle.getDataFormatString();
        if (formatString == null) {
            formatString = BuiltinFormats.getBuiltinFormat(formatIndex);
        }
        final DataFormatter formatter = new DataFormatter();
        return formatter.formatRawCellContents(cellValue, formatIndex, formatString);
    }
}
