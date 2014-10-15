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

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

/**
 * An iterator implementation that iterates from the first logical (as opposed
 * to physical, which is the default in POI) row in a spreadsheet.
 */
final class ZeroBasedRowIterator implements Iterator<Row> {

	private final Sheet _sheet;
	private volatile int _rowNumber;

	public ZeroBasedRowIterator(Sheet sheet) {
		_sheet = sheet;
		_rowNumber = -1;
	}

	@Override
	public boolean hasNext() {
		return _rowNumber < _sheet.getLastRowNum();
	}

	@Override
	public Row next() {
		_rowNumber++;
		return _sheet.getRow(_rowNumber);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove() is not supported");
	}

}
