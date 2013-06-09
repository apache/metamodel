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

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

/**
 * An iterator implementation that iterates from the first logical (as opposed
 * to physical, which is the default in POI) row in a spreadsheet.
 * 
 * @author Kasper Sørensen
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
