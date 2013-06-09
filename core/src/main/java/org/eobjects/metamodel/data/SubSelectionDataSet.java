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

import org.eobjects.metamodel.query.SelectItem;

/**
 * {@link DataSet} wrapper for doing subselection.
 * 
 * @author Kasper Sørensen
 */
public final class SubSelectionDataSet extends AbstractDataSet {

    private final DataSet _dataSet;

    public SubSelectionDataSet(SelectItem[] selectItemsArray, DataSet dataSet) {
        super(selectItemsArray);
        _dataSet = dataSet;
    }

    public DataSet getWrappedDataSet() {
        return _dataSet;
    }

    @Override
    public boolean next() {
        return _dataSet.next();
    }

    @Override
    public Row getRow() {
        final DataSetHeader header = getHeader();
        return _dataSet.getRow().getSubSelection(header);
    }

    @Override
    public void close() {
        super.close();
        _dataSet.close();
    }
}
