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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.RowPublisherDataSet;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.AlphabeticSequence;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

/**
 * {@link SpreadsheetReaderDelegate} implementation for the "new" XLSX format.
 * This implementation is very efficient as it uses SAX XML parsing which does
 * not bloat memory usage in the same way that POI's user model does.
 * 
 * @author Kasper Sørensen
 */
final class XlsxSpreadsheetReaderDelegate implements SpreadsheetReaderDelegate {

    private static final Logger logger = LoggerFactory.getLogger(XlsxSpreadsheetReaderDelegate.class);

    private final ExcelConfiguration _configuration;
    private final Map<String, String> _tableNamesToInternalIds;

    public XlsxSpreadsheetReaderDelegate(ExcelConfiguration configuration) {
        _configuration = configuration;
        _tableNamesToInternalIds = new HashMap<String, String>();
    }

    @Override
    public DataSet executeQuery(InputStream inputStream, Table table, Column[] columns, int maxRows) throws Exception {
        final OPCPackage pkg = OPCPackage.open(inputStream);
        final XSSFReader xssfReader = new XSSFReader(pkg);
        final String relationshipId = _tableNamesToInternalIds.get(table.getName());

        return buildDataSet(columns, maxRows, relationshipId, xssfReader);
    }

    @Override
    public Schema createSchema(InputStream inputStream, String schemaName) throws Exception {
        final MutableSchema schema = new MutableSchema(schemaName);
        final OPCPackage pkg = OPCPackage.open(inputStream);
        final XSSFReader xssfReader = new XSSFReader(pkg);

        final XlsxWorkbookToTablesHandler workbookToTables = new XlsxWorkbookToTablesHandler(schema,
                _tableNamesToInternalIds);
        buildTables(xssfReader, workbookToTables);

        for (Entry<String, String> entry : _tableNamesToInternalIds.entrySet()) {

            final String tableName = entry.getKey();
            final String relationshipId = entry.getValue();

            final MutableTable table = (MutableTable) schema.getTableByName(tableName);

            buildColumns(table, relationshipId, xssfReader);
        }
        return schema;
    }

    @Override
    public void notifyTablesModified(Ref<InputStream> inputStreamRef) {
        InputStream inputStream = inputStreamRef.get();
        final XlsxWorkbookToTablesHandler workbookToTables = new XlsxWorkbookToTablesHandler(null,
                _tableNamesToInternalIds);
        try {
            final OPCPackage pkg = OPCPackage.open(inputStream);
            final XSSFReader xssfReader = new XSSFReader(pkg);
            buildTables(xssfReader, workbookToTables);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            FileHelper.safeClose(inputStream);
        }
    }

    private DataSet buildDataSet(final Column[] columns, int maxRows, final String relationshipId,
            final XSSFReader xssfReader) throws Exception {

        List<SelectItem> selectItems = new ArrayList<SelectItem>(columns.length);
        for (Column column : columns) {
            selectItems.add(new SelectItem(column));
        }
        final XlsxRowPublisherAction publishAction = new XlsxRowPublisherAction(_configuration, columns,
                relationshipId, xssfReader);

        return new RowPublisherDataSet(selectItems.toArray(new SelectItem[selectItems.size()]), maxRows, publishAction);
    }

    private void buildColumns(final MutableTable table, final String relationshipId, final XSSFReader xssfReader)
            throws Exception {
        final InputStream sheetData = xssfReader.getSheet(relationshipId);

        final XlsxRowCallback rowCallback = new XlsxRowCallback() {
            @Override
            public boolean row(int rowNumber, List<String> values, List<Style> styles) {
                final int columnNameLineNumber = _configuration.getColumnNameLineNumber();
                if (columnNameLineNumber == ExcelConfiguration.NO_COLUMN_NAME_LINE) {
                    AlphabeticSequence alphabeticSequence = new AlphabeticSequence();
                    List<String> generatedColumnNames = new ArrayList<String>(values.size());
                    for (String originalColumnName : values) {
                        String columnName = alphabeticSequence.next();
                        if (originalColumnName == null) {
                            columnName = null;
                        }
                        generatedColumnNames.add(columnName);
                    }
                    buildColumns(table, generatedColumnNames);
                    return false;
                } else {
                    final int zeroBasedLineNumber = columnNameLineNumber - 1;
                    if (rowNumber >= zeroBasedLineNumber) {
                        buildColumns(table, values);
                        return false;
                    }
                }
                return true;
            }
        };
        final XlsxSheetToRowsHandler handler = new XlsxSheetToRowsHandler(rowCallback, xssfReader, _configuration);

        final XMLReader sheetParser = ExcelUtils.createXmlReader();
        sheetParser.setContentHandler(handler);
        try {
            sheetParser.parse(new InputSource(sheetData));
        } catch (XlsxStopParsingException e) {
            logger.debug("Parsing stop signal thrown");
        } finally {
            FileHelper.safeClose(sheetData);
        }
    }

    protected void buildColumns(final MutableTable table, final List<String> columnNames) {
        int columnNumber = 0;
        for (String columnName : columnNames) {
            if (columnName != null || !_configuration.isSkipEmptyColumns()) {
                if (columnName == null) {
                    columnName = "[Column " + (columnNumber + 1) + "]";
                }
                table.addColumn(new MutableColumn(columnName, ColumnType.VARCHAR, table, columnNumber, true));
            }
            columnNumber++;
        }
    }

    private void buildTables(final XSSFReader xssfReader, final XlsxWorkbookToTablesHandler workbookToTables)
            throws Exception {
        final InputStream workbookData = xssfReader.getWorkbookData();
        final XMLReader workbookParser = ExcelUtils.createXmlReader();
        workbookParser.setContentHandler(workbookToTables);
        workbookParser.parse(new InputSource(workbookData));
        FileHelper.safeClose(workbookData);
    }
}
