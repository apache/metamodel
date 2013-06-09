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
import java.util.List;

import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.eobjects.metamodel.data.RowPublisher;
import org.eobjects.metamodel.data.Style;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.util.Action;
import org.eobjects.metamodel.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

class XlsxRowPublisherAction implements Action<RowPublisher> {

	private static final Logger logger = LoggerFactory
			.getLogger(XlsxRowPublisherAction.class);

	private final ExcelConfiguration _configuration;
	private final Column[] _columns;
	private final String _relationshipId;
	private final XSSFReader _xssfReader;

	public XlsxRowPublisherAction(ExcelConfiguration configuration,
			Column[] columns, String relationshipId, XSSFReader xssfReader) {
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

				Object[] rowData = new Object[_columns.length];
				Style[] styleData = new Style[_columns.length];
				for (int i = 0; i < _columns.length; i++) {
					int columnNumber = _columns[i].getColumnNumber();
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