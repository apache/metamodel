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
 * Wraps another DataSet and enforces a first row offset.
 */
public final class FirstRowDataSet extends AbstractDataSet {

    private final DataSet _dataSet;
    private volatile int _rowsLeftToSkip;

    /**
     * Constructs a {@link FirstRowDataSet}.
     * 
     * @param dataSet
     *            the dataset to wrap
     * @param firstRow
     *            the first row number (1-based).
     */
    public FirstRowDataSet(DataSet dataSet, int firstRow) {
        super(dataSet);
        _dataSet = dataSet;
        if (firstRow < 1) {
            throw new IllegalArgumentException("First row cannot be negative or zero");
        }
        _rowsLeftToSkip = firstRow - 1;
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
        boolean next = true;
        if (_rowsLeftToSkip > 0) {
            while (_rowsLeftToSkip > 0) {
                next = _dataSet.next();
                if (next) {
                    _rowsLeftToSkip--;
                } else {
                    // no more rows at all - exit loop
                    _rowsLeftToSkip = 0;
                    return false;
                }
            }
        }
        return _dataSet.next();
    }
}