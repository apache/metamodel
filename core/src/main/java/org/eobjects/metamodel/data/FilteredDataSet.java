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


/**
 * Wraps another DataSet and transparently applies a set of filters to it.
 * 
 * @author Kasper Sørensen
 */
public final class FilteredDataSet extends AbstractDataSet {

	private final DataSet _dataSet;
	private final IRowFilter[] _filters;
	private Row _row;

	public FilteredDataSet(DataSet dataSet, IRowFilter... filters) {
	    super(dataSet);
		_dataSet = dataSet;
		_filters = filters;
	}

	@Override
	public void close() {
		super.close();
		_dataSet.close();
	}

	@Override
	public boolean next() {
		boolean next = false;
		while (_dataSet.next()) {
			Row row = _dataSet.getRow();
			for (IRowFilter filter : _filters) {
				next = filter.accept(row);
				if (!next) {
					break;
				}
			}
			if (next) {
				_row = row;
				break;
			}
		}
		return next;
	}

	@Override
	public Row getRow() {
		return _row;
	}
}