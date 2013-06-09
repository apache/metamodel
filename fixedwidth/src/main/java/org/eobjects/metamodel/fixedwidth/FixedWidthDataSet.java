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
package org.eobjects.metamodel.fixedwidth;

import org.eobjects.metamodel.data.AbstractDataSet;
import org.eobjects.metamodel.data.DefaultRow;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.util.FileHelper;

/**
 * DataSet implementation for fixed width values.
 * 
 * @author Kasper Sørensen
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
				// Ticket #125: Missing values should be enterpreted as
				// null.
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