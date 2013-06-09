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
package org.eobjects.metamodel.pojo;

import java.util.Iterator;

import org.eobjects.metamodel.data.AbstractDataSet;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.DefaultRow;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.SelectItem;

/**
 * {@link DataSet} implementation that passes on most responsibility to
 * {@link TableDataProvider#getValue(String, Object)}.
 * 
 * @param <E>
 */
final class PojoDataSet<E> extends AbstractDataSet {

    private final TableDataProvider<E> _pojoTable;
    private final Iterator<E> _iterator;
    private E _next;

    public PojoDataSet(TableDataProvider<E> pojoTable, SelectItem[] selectItems) {
        super(selectItems);
        _pojoTable = pojoTable;

        _iterator = _pojoTable.iterator();
    }

    @Override
    public boolean next() {
        if (_iterator.hasNext()) {
            _next = _iterator.next();
            return true;
        } else {
            _next = null;
            return false;
        }
    }

    @Override
    public Row getRow() {
        final int size = getHeader().size();
        final Object[] values = new Object[size];

        for (int i = 0; i < values.length; i++) {
            final SelectItem selectItem = getHeader().getSelectItem(i);
            final String column = selectItem.getColumn().getName();
            values[i] = _pojoTable.getValue(column, _next);
        }

        return new DefaultRow(getHeader(), values);
    }

    /**
     * Used by DELETE statements to delete a record.
     */
    protected void remove() {
        _iterator.remove();
    }
}
