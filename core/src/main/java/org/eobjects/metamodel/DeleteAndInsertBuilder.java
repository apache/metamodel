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
package org.eobjects.metamodel;

import java.util.List;
import java.util.ListIterator;

import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.DefaultRow;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.data.SimpleDataSetHeader;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.update.AbstractRowUpdationBuilder;
import org.eobjects.metamodel.update.RowUpdationBuilder;

/**
 * Simple implementation of the {@link RowUpdationBuilder} interface, which
 * simply uses a combined delete+insert strategy for performing updates. Note
 * that this implementation is not desirable performance-wise in many cases, but
 * does provide a functional equivalent to a "real" update.
 */
public class DeleteAndInsertBuilder extends AbstractRowUpdationBuilder {

    private final AbstractUpdateCallback _updateCallback;

    public DeleteAndInsertBuilder(AbstractUpdateCallback updateCallback, Table table) {
        super(table);
        assert updateCallback.isInsertSupported();
        assert updateCallback.isDeleteSupported();
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {
        // retain rows in memory
        List<Row> rows = getRowsToUpdate();

        // delete rows
        _updateCallback.deleteFrom(getTable()).where(getWhereItems()).execute();

        // modify rows
        rows = updateRows(rows);

        // insert rows
        for (Row row : rows) {
            _updateCallback.insertInto(getTable()).like(row).execute();
        }
    }

    private List<Row> updateRows(List<Row> rows) {
        for (ListIterator<Row> it = rows.listIterator(); it.hasNext();) {
            final Row original = (Row) it.next();
            final Row updated = update(original);
            it.set(updated);
        }
        return rows;
    }

    /**
     * Produces an updated row out of the original
     * 
     * @param original
     * @return
     */
    private Row update(final Row original) {
        SelectItem[] items = original.getSelectItems();
        Object[] values = new Object[items.length];
        for (int i = 0; i < items.length; i++) {
            final Object value;
            Column column = items[i].getColumn();
            if (isSet(column)) {
                // use update statement's value
                value = getValues()[i];
            } else {
                // use original value
                value = original.getValue(i);
            }
            values[i] = value;
        }
        return new DefaultRow(new SimpleDataSetHeader(items), values);
    }

    protected List<Row> getRowsToUpdate() {
        final DataContext dc = _updateCallback.getDataContext();
        final Table table = getTable();
        final List<FilterItem> whereItems = getWhereItems();
        final DataSet dataSet = dc.query().from(table).select(table.getColumns()).where(whereItems).execute();
        final List<Row> rows = dataSet.toRows();
        return rows;
    }

}
