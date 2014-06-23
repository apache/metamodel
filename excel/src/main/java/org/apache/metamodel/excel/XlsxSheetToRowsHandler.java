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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.FontUnderline;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.data.Style.SizeUnit;
import org.apache.metamodel.data.StyleBuilder;
import org.apache.metamodel.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * XML handler for transforming a sheet into rows. Uses an
 * {@link XlsxRowCallback} to publish identified rows.
 */
final class XlsxSheetToRowsHandler extends DefaultHandler {

    private static final Logger logger = LoggerFactory.getLogger(XlsxSheetToRowsHandler.class);

    private static enum XssfDataType {
        BOOL, ERROR, FORMULA, INLINESTR, SSTINDEX, NUMBER,
    }

    // global variables
    private final XlsxRowCallback _callback;
    private final ExcelConfiguration _configuration;
    private final StylesTable _stylesTable;
    private final SharedStringsTable _sharedStringTable;

    // variables used to hold information about the current rows
    private int _rowNumber;
    private final List<String> _rowValues;
    private final List<Style> _styles;

    // variables used to hold information about the current visited cells
    private final StringBuilder _value;
    private final StyleBuilder _style;
    private boolean _inCell;
    private boolean _inFormula;
    private int _columnNumber;
    private XssfDataType _dataType;
    private int _formatIndex;
    private String _formatString;

    public XlsxSheetToRowsHandler(XlsxRowCallback callback, XSSFReader xssfReader, ExcelConfiguration configuration)
            throws Exception {
        _callback = callback;
        _configuration = configuration;

        _sharedStringTable = xssfReader.getSharedStringsTable();
        _stylesTable = xssfReader.getStylesTable();

        _value = new StringBuilder();
        _style = new StyleBuilder();
        _rowValues = new ArrayList<String>();
        _styles = new ArrayList<Style>();
        _rowNumber = -1;
        _inCell = false;
        _inFormula = false;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if ("row".equals(qName)) {
            // element is a row

            // excel row numbers are 1-based
            int rowNumber = Integer.parseInt(attributes.getValue("r"));
            rowNumber = rowNumber - 1;

            if (_configuration.isSkipEmptyLines()) {
                _rowNumber++;
            } else {
                while (_rowNumber + 1 < rowNumber) {
                    // empty lines are not skipped, so dispatch empty lines
                    _rowNumber++;
                    List<String> emptyValues = Collections.emptyList();
                    List<Style> emptyStyles = Collections.emptyList();
                    _callback.row(_rowNumber, emptyValues, emptyStyles);
                }
                _rowNumber = rowNumber;
            }
        } else if ("c".equals(qName)) {
            // element is a cell

            _inCell = true;

            final String r = attributes.getValue("r");
            int firstDigit = -1;
            for (int c = 0; c < r.length(); ++c) {
                if (Character.isDigit(r.charAt(c))) {
                    firstDigit = c;
                    break;
                }
            }
            _columnNumber = nameToColumn(r.substring(0, firstDigit));

            // Set up defaults.
            _dataType = XssfDataType.NUMBER;
            _formatIndex = -1;
            _formatString = null;

            final String cellType = attributes.getValue("t");
            if ("b".equals(cellType)) {
                _dataType = XssfDataType.BOOL;
            } else if ("e".equals(cellType)) {
                _dataType = XssfDataType.ERROR;
            } else if ("inlineStr".equals(cellType)) {
                _dataType = XssfDataType.INLINESTR;
            } else if ("s".equals(cellType)) {
                _dataType = XssfDataType.SSTINDEX;
            } else if ("str".equals(cellType)) {
                _dataType = XssfDataType.FORMULA;
            }

            String cellStyleStr = attributes.getValue("s");
            if (cellStyleStr != null) {
                // It's a number, but almost certainly one
                // with a special style or format
                int styleIndex = Integer.parseInt(cellStyleStr);
                XSSFCellStyle style = _stylesTable.getStyleAt(styleIndex);

                configureStyle(style);

                if (_dataType == XssfDataType.NUMBER) {
                    this._formatIndex = style.getDataFormat();
                    this._formatString = style.getDataFormatString();
                    if (this._formatString == null) {
                        this._formatString = BuiltinFormats.getBuiltinFormat(this._formatIndex);
                    }
                }
            }
        } else if (_inCell && "f".equals(qName)) {
            // skip the actual formula line
            _inFormula = true;
        }
    }

    private void configureStyle(XSSFCellStyle style) {
        XSSFFont font = style.getFont();
        if (font.getBold()) {
            _style.bold();
        }
        if (font.getItalic()) {
            _style.italic();
        }
        if (font.getUnderline() != FontUnderline.NONE.getByteValue()) {
            _style.underline();
        }

        if (style.getFillPatternEnum() == FillPatternType.SOLID_FOREGROUND) {
            XSSFColor fillForegroundXSSFColor = style.getFillForegroundXSSFColor();
            String argb = fillForegroundXSSFColor.getARGBHex();
            if (argb != null) {
                _style.background(argb.substring(2));
            }
        }

        final XSSFFont stdFont = _stylesTable.getStyleAt(0).getFont();
        final short fontHeight = style.getFont().getFontHeightInPoints();
        if (stdFont.getFontHeightInPoints() != fontHeight) {
            _style.fontSize(fontHeight, SizeUnit.PT);
        }

        XSSFColor fontColor = style.getFont().getXSSFColor();
        if (fontColor != null) {
            String argbHex = fontColor.getARGBHex();
            if (argbHex != null) {
                _style.foreground(argbHex.substring(2));
            }
        }

        switch (style.getAlignmentEnum()) {
        case LEFT:
            _style.leftAligned();
            break;
        case RIGHT:
            _style.rightAligned();
            break;
        case CENTER:
            _style.centerAligned();
            break;
        case JUSTIFY:
            _style.justifyAligned();
            break;
        default:
            // do nothing
            break;
        }

    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if ("row".equals(qName)) {
            // element was a row
            boolean next = _callback.row(_rowNumber, _rowValues, _styles);
            if (!next) {
                throw new XlsxStopParsingException();
            }
            _rowValues.clear();
            _styles.clear();
        } else if ("c".equals(qName)) {
            // element was a cell

            _inCell = false;

            while (_rowValues.size() < _columnNumber) {
                _rowValues.add(null);
                _styles.add(Style.NO_STYLE);
            }

            _rowValues.add(createValue());
            _styles.add(_style.create());
            _value.setLength(0);
            _style.reset();
        } else if (_inFormula && "f".equals(qName)) {
            // skip the actual formula line
            _inFormula = false;
        }
    }

    private String createValue() {
        if (_value.length() == 0) {
            return null;
        }

        switch (_dataType) {

        case BOOL:
            char first = _value.charAt(0);
            return first == '0' ? "false" : "true";
        case ERROR:
            logger.warn("Error-cell occurred: {}", _value);
            return _value.toString();
        case FORMULA:
            return _value.toString();
        case INLINESTR:
            XSSFRichTextString rtsi = new XSSFRichTextString(_value.toString());
            return rtsi.toString();
        case SSTINDEX:
            String sstIndex = _value.toString();
            int idx = Integer.parseInt(sstIndex);
            XSSFRichTextString rtss = new XSSFRichTextString(_sharedStringTable.getEntryAt(idx));
            return rtss.toString();
        case NUMBER:
            final String numberString = _value.toString();
            if (_formatString != null) {
                DataFormatter formatter = getDataFormatter();
                if (HSSFDateUtil.isADateFormat(_formatIndex, _formatString)) {
                    Date date = DateUtil.getJavaDate(Double.parseDouble(numberString));
                    return DateUtils.createDateFormat().format(date);
                }
                return formatter.formatRawCellContents(Double.parseDouble(numberString), _formatIndex, _formatString);
            } else {
                if (numberString.endsWith(".0")) {
                    // xlsx only stores doubles, so integers get ".0" appended
                    // to them
                    return numberString.substring(0, numberString.length() - 2);
                }
                return numberString;
            }
        default:
            logger.error("Unsupported data type: {}", _dataType);
            return "";
        }
    }

    private DataFormatter getDataFormatter() {
        return new DataFormatter();
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (_inCell && !_inFormula) {
            _value.append(ch, start, length);
        }
    }

    /**
     * Converts an Excel column name like "C" to a zero-based index.
     * 
     * @param name
     * @return Index corresponding to the specified name
     */
    private int nameToColumn(String name) {
        int column = -1;
        for (int i = 0; i < name.length(); ++i) {
            int c = name.charAt(i);
            column = (column + 1) * 26 + c - 'A';
        }
        return column;
    }
}
