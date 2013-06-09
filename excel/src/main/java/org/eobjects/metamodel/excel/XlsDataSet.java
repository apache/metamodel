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

import java.util.Iterator;

import org.apache.poi.ss.usermodel.Workbook;
import org.eobjects.metamodel.data.AbstractDataSet;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.SelectItem;

/**
 * Stream {@link DataSet} implementation for Excel support.
 * 
 * @author Kasper Sørensen
 */
final class XlsDataSet extends AbstractDataSet {

	private final Iterator<org.apache.poi.ss.usermodel.Row> _rowIterator;
	private final Workbook _workbook;

	private volatile org.apache.poi.ss.usermodel.Row _row;
	private volatile boolean _closed;

	/**
	 * Creates an XLS dataset
	 * 
	 * @param selectItems
	 *            the selectitems representing the columns of the table
	 * @param workbook
	 * @param rowIterator
	 */
	public XlsDataSet(SelectItem[] selectItems, Workbook workbook,
			Iterator<org.apache.poi.ss.usermodel.Row> rowIterator) {
	    super(selectItems);
		_workbook = workbook;
		_rowIterator = rowIterator;
		_closed = false;
	}

	@Override
	public boolean next() {
		if (_rowIterator.hasNext()) {
			_row = _rowIterator.next();
			return true;
		} else {
			_row = null;
			_closed = true;
			return false;
		}
	}

	@Override
	public Row getRow() {
		if (_closed) {
			return null;
		}

		return ExcelUtils.createRow(_workbook, _row, getHeader());
	}
}
