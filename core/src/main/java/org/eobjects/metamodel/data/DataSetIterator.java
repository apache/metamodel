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
package org.eobjects.metamodel.data;

import java.util.Iterator;

/**
 * Iterator implementation that iterates through a DataSet.
 * 
 * @author Kasper Sørensen
 */
public final class DataSetIterator implements Iterator<Row> {

	private final DataSet _dataSet;
	private volatile short _iterationState;
	private volatile Row _row;

	public DataSetIterator(DataSet dataSet) {
		_dataSet = dataSet;
		// 0 = uninitialized, 1=row not read yet, 2=row read, 3=finished
		_iterationState = 0;
	}

	@Override
	public boolean hasNext() {
		if (_iterationState == 0 || _iterationState == 2) {
			if (_dataSet.next()) {
				_iterationState = 1;
				_row = _dataSet.getRow();
			} else {
				_iterationState = 3;
				_row = null;
				_dataSet.close();
			}
		}
		return _iterationState == 1;
	}

	@Override
	public Row next() {
		if (_iterationState == 1) {
			_iterationState = 2;
		}
		return _row;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException(
				"DataSet is read-only, remove() is not supported.");
	}

}
