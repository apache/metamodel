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
package org.apache.metamodel.fixedwidth;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.util.FileHelper;

/**
 * DataSet implementation for fixed width values.
 */
class FixedWidthDataSet extends AbstractDataSet {

	private final FixedWidthReader _reader;
	private volatile Integer _rowsRemaining;
	private volatile Row _row;

	public FixedWidthDataSet(FixedWidthReader reader, Column[] columns,
			Integer maxRows) {
		super(columns);
		_reader = reader;
		_rowsRemaining = maxRows;
	}

	@Override
	public void close() {
		FileHelper.safeClose(_reader);
		_row = null;
		_rowsRemaining = null;
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		// close is always safe to invoke
		close();
	}

	@Override
	public Row getRow() {
		return _row;
	}

	@Override
	public boolean next() {
		if (_rowsRemaining != null && _rowsRemaining > 0) {
			_rowsRemaining--;
			return nextInternal();
		} else if (_rowsRemaining == null) {
			return nextInternal();
		} else {
			return false;
		}
	}

	private boolean nextInternal() {
		if (_reader == null) {
			return false;
		}

		InconsistentValueWidthException exception;
		String[] stringValues;
		try {
			stringValues = _reader.readLine();
			exception = null;
		} catch (InconsistentValueWidthException e) {
			stringValues = e.getSourceResult();
			exception = e;
		}
		if (stringValues == null) {
			close();
			return false;
		}
		
		final int size = getHeader().size();
        Object[] rowValues = new Object[size];
		for (int i = 0; i < size; i++) {
			Column column = getHeader().getSelectItem(i).getColumn();
			int columnNumber = column.getColumnNumber();
			if (columnNumber < stringValues.length) {
				rowValues[i] = stringValues[columnNumber];
			} else {
				// Ticket #125: Missing values should be interpreted as null.
				rowValues[i] = null;
			}
		}
		_row = new DefaultRow(getHeader(), rowValues);

		if (exception != null) {
			throw new InconsistentValueWidthException(_row, exception);
		}
		return true;
	}
}