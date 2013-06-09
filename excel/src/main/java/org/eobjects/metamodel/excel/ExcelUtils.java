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
import java.io.OutputStream;
import java.text.NumberFormat;
import java.util.Date;
import java.util.Iterator;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.formula.FormulaParseException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Color;
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
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.MetaModelHelper;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.DataSetHeader;
import org.eobjects.metamodel.data.DefaultRow;
import org.eobjects.metamodel.data.EmptyDataSet;
import org.eobjects.metamodel.data.Style;
import org.eobjects.metamodel.data.Style.SizeUnit;
import org.eobjects.metamodel.data.StyleBuilder;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.Action;
import org.eobjects.metamodel.util.DateUtils;
import org.eobjects.metamodel.util.FormatHelper;
import org.eobjects.metamodel.util.Func;
import org.eobjects.metamodel.util.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.XMLReader;

/**
 * Convenience/reusable methods for Excel workbook handling.
 * 
 * @author Kasper Sørensen
 */
final class ExcelUtils {

    private static final Logger logger = LoggerFactory.getLogger(ExcelUtils.class);

    private static final NumberFormat _numberFormat = FormatHelper.getUiNumberFormat();

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
     * Initializes a workbook instance based on a inputstream.
     * 
     * @return a workbook instance based on the inputstream.
     */
    public static Workbook readWorkbook(InputStream inputStream) {
        try {
            return WorkbookFactory.create(inputStream);
        } catch (Exception e) {
            logger.error("Could not open workbook", e);
            throw new IllegalStateException("Could not open workbook", e);
        }
    }

    public static Workbook readWorkbook(Resource resource) {
        return resource.read(new Func<InputStream, Workbook>() {
            @Override
            public Workbook eval(InputStream inputStream) {
                return readWorkbook(inputStream);
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
    public static Workbook readWorkbook(ExcelDataContext dataContext) {
        Resource resource = dataContext.getResource();
        if (!resource.isExists()) {
            if (isXlsxFile(resource)) {
                return new SXSSFWorkbook(1000);
            } else {
                return new HSSFWorkbook();
            }
        }
        return readWorkbook(resource);
    }

    public static void writeWorkbook(ExcelDataContext dataContext, final Workbook wb) {
        final Resource resource = dataContext.getResource();
        resource.write(new Action<OutputStream>() {
            @Override
            public void run(OutputStream outputStream) throws Exception {
                wb.write(outputStream);
            }
        });
    }

    public static String getCellValue(Workbook wb, Cell cell) {
        if (cell == null) {
            return null;
        }

        final String cellCoordinate = "(" + cell.getRowIndex() + "," + cell.getColumnIndex() + ")";

        final String result;

        switch (cell.getCellType()) {
        case Cell.CELL_TYPE_BLANK:
            result = null;
            break;
        case Cell.CELL_TYPE_BOOLEAN:
            result = Boolean.toString(cell.getBooleanCellValue());
            break;
        case Cell.CELL_TYPE_ERROR:
            String errorResult;
            try {
                byte errorCode = cell.getErrorCellValue();
                FormulaError formulaError = FormulaError.forInt(errorCode);
                errorResult = formulaError.getString();
            } catch (RuntimeException e) {
                logger.debug("Getting error code for {} failed!: {}", cellCoordinate, e.getMessage());
                if (cell instanceof XSSFCell) {
                    // hack to get error string, which is available
                    String value = ((XSSFCell) cell).getErrorCellString();
                    errorResult = value;
                } else {
                    logger.error("Couldn't handle unexpected error scenario in cell: " + cellCoordinate, e);
                    throw e;
                }
            }
            result = errorResult;
            break;
        case Cell.CELL_TYPE_FORMULA:
            // result = cell.getCellFormula();
            result = getFormulaCellValue(wb, cell);
            break;
        case Cell.CELL_TYPE_NUMERIC:
            if (HSSFDateUtil.isCellDateFormatted(cell)) {
                Date date = cell.getDateCellValue();
                if (date == null) {
                    result = null;
                } else {
                    result = DateUtils.createDateFormat().format(date);
                }
            } else {
                // TODO: Consider not formatting it, but simple using
                // Double.toString(...)
                result = _numberFormat.format(cell.getNumericCellValue());
            }
            break;
        case Cell.CELL_TYPE_STRING:
            result = cell.getRichStringCellValue().getString();
            break;
        default:
            throw new IllegalStateException("Unknown cell type: " + cell.getCellType());
        }

        logger.debug("cell {} resolved to value: {}", cellCoordinate, result);

        return result;
    }

    private static String getFormulaCellValue(Workbook wb, Cell cell) {
        // first try with a cached/precalculated value
        try {
            double numericCellValue = cell.getNumericCellValue();
            // TODO: Consider not formatting it, but simple using
            // Double.toString(...)
            return _numberFormat.format(numericCellValue);
        } catch (Exception e) {
            if (logger.isInfoEnabled()) {
                logger.info("Failed to fetch cached/precalculated formula value of cell: " + cell, e);
            }
        }

        // evaluate cell first, if possible
        try {
            if (logger.isInfoEnabled()) {
                logger.info("cell({},{}) is a formula. Attempting to evaluate: {}",
                        new Object[] { cell.getRowIndex(), cell.getColumnIndex(), cell.getCellFormula() });
            }

            final FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();

            // calculates the formula and puts it's value back into the cell
            final Cell evaluatedCell = evaluator.evaluateInCell(cell);

            return getCellValue(wb, evaluatedCell);
        } catch (RuntimeException e) {
            logger.warn("Exception occurred while evaluating formula at position ({},{}): {}", new Object[] { cell.getRowIndex(),
                    cell.getColumnIndex(), e.getMessage() });
            // Some exceptions we simply log - result will be then be the
            // actual formula
            if (e instanceof FormulaParseException) {
                logger.error("Parse exception occurred while evaluating cell formula: " + cell, e);
            } else if (e instanceof IllegalArgumentException) {
                logger.error("Illegal formula argument occurred while evaluating cell formula: " + cell, e);
            } else {
                logger.error("Unexpected exception occurred while evaluating cell formula: " + cell, e);
            }
        }

        // last resort: return the string formula
        return cell.getCellFormula();
    }

    public static Style getCellStyle(Workbook workbook, Cell cell) {
        if (cell == null) {
            return Style.NO_STYLE;
        }
        final CellStyle cellStyle = cell.getCellStyle();

        final short fontIndex = cellStyle.getFontIndex();
        final Font font = workbook.getFontAt(fontIndex);
        final StyleBuilder styleBuilder = new StyleBuilder();

        // Font bold, italic, underline
        if (font.getBoldweight() >= Font.BOLDWEIGHT_BOLD) {
            styleBuilder.bold();
        }
        if (font.getItalic()) {
            styleBuilder.italic();
        }
        if (font.getUnderline() != FontUnderline.NONE.getByteValue()) {
            styleBuilder.underline();
        }

        // Font size
        final Font stdFont = workbook.getFontAt((short) 0);
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
        if (cellStyle.getFillPattern() == 1) {
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
                throw new IllegalStateException("Unexpected color type: " + (color == null ? "null" : color.getClass()) + ")");
            }
        }

        // alignment
        switch (cellStyle.getAlignment()) {
        case CellStyle.ALIGN_LEFT:
            styleBuilder.leftAligned();
            break;
        case CellStyle.ALIGN_RIGHT:
            styleBuilder.rightAligned();
            break;
        case CellStyle.ALIGN_CENTER:
            styleBuilder.centerAligned();
            break;
        case CellStyle.ALIGN_JUSTIFY:
            styleBuilder.justifyAligned();
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
     * @param selectItems
     *            select items of the columns in the table
     * @return
     */
    public static DefaultRow createRow(Workbook workbook, Row row, DataSetHeader header) {
        final int size = header.size();
        final String[] values = new String[size];
        final Style[] styles = new Style[size];
        if (row != null) {
            for (int i = 0; i < size; i++) {
                final int columnNumber = header.getSelectItem(i).getColumn().getColumnNumber();
                final Cell cell = row.getCell(columnNumber);
                final String value = ExcelUtils.getCellValue(workbook, cell);
                final Style style = ExcelUtils.getCellStyle(workbook, cell);
                values[i] = value;
                styles[i] = style;
            }
        }

        return new DefaultRow(header, values, styles);
    }

    public static DataSet getDataSet(Workbook workbook, Sheet sheet, Table table, ExcelConfiguration configuration) {
        final SelectItem[] selectItems = MetaModelHelper.createSelectItems(table.getColumns());
        final Iterator<Row> rowIterator = getRowIterator(sheet, configuration, true);
        if (!rowIterator.hasNext()) {
            // no more rows!
            return new EmptyDataSet(selectItems);
        }

        final DataSet dataSet = new XlsDataSet(selectItems, workbook, rowIterator);
        return dataSet;
    }
}
