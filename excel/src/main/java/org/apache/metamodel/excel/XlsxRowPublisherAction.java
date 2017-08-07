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
import java.util.List;

import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.metamodel.data.RowPublisher;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.util.Action;
import org.apache.metamodel.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

class XlsxRowPublisherAction implements Action<RowPublisher> {

	private static final Logger logger = LoggerFactory
			.getLogger(XlsxRowPublisherAction.class);

	private final ExcelConfiguration _configuration;
	private final List<Column> _columns;
	private final String _relationshipId;
	private final XSSFReader _xssfReader;

	public XlsxRowPublisherAction(ExcelConfiguration configuration,
			List<Column> columns, String relationshipId, XSSFReader xssfReader) {
		_configuration = configuration;
		_columns = columns;
		_relationshipId = relationshipId;
		_xssfReader = xssfReader;
	}

	@Override
	public void run(final RowPublisher publisher) throws Exception {
		final InputStream sheetData = _xssfReader.getSheet(_relationshipId);

		final XlsxRowCallback rowCallback = new XlsxRowCallback() {
			@Override
			public boolean row(int rowNumber, List<String> values,
					List<Style> styles) {
				if (_configuration.getColumnNameLineNumber() != ExcelConfiguration.NO_COLUMN_NAME_LINE) {
					final int zeroBasedLineNumber = _configuration.getColumnNameLineNumber() - 1;
                    if (rowNumber <= zeroBasedLineNumber) {
						// skip header rows
						return true;
					}
				}

				Object[] rowData = new Object[_columns.size()];
				Style[] styleData = new Style[_columns.size()];
				for (int i = 0; i < _columns.size(); i++) {
					int columnNumber = _columns.get(i).getColumnNumber();
					if (columnNumber < values.size()) {
						rowData[i] = values.get(columnNumber);
						styleData[i] = styles.get(columnNumber);
					} else {
						rowData[i] = null;
						styleData[i] = Style.NO_STYLE;
					}
				}

				return publisher.publish(rowData, styleData);
			}
		};
		final XlsxSheetToRowsHandler handler = new XlsxSheetToRowsHandler(
				rowCallback, _xssfReader, _configuration);

		final XMLReader sheetParser = ExcelUtils.createXmlReader();
		sheetParser.setContentHandler(handler);
		sheetParser.setErrorHandler(handler);
		try {
			sheetParser.parse(new InputSource(sheetData));
		} catch (XlsxStopParsingException e) {
			logger.debug("Parsing stop signal thrown");
		} catch (Exception e) {
			logger.warn("Unexpected error occurred while parsing", e);
			throw e;
		} finally {
			publisher.finished();
			FileHelper.safeClose(sheetData);
		}
	}
}