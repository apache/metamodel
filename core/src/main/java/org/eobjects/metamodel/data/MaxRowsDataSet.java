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
 * Wraps another DataSet and enforces a maximum number of rows constraint
 */
public final class MaxRowsDataSet extends AbstractDataSet {

    private final DataSet _dataSet;
    private volatile int _rowsLeft;

    public MaxRowsDataSet(DataSet dataSet, int maxRows) {
        super(dataSet);
        _dataSet = dataSet;
        _rowsLeft = maxRows;
    }

    @Override
    public void close() {
        _dataSet.close();
    }

    @Override
    public Row getRow() {
        return _dataSet.getRow();
    }

    @Override
    public boolean next() {
        if (_rowsLeft > 0) {
            boolean next = _dataSet.next();
            if (next) {
                _rowsLeft--;
            }
            return next;
        }
        return false;
    }
}