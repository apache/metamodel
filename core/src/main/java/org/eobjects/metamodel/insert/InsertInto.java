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
package org.eobjects.metamodel.insert;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.data.AbstractRowBuilder;
import org.eobjects.metamodel.data.RowBuilder;
import org.eobjects.metamodel.data.Style;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;

/**
 * Represents a single INSERT INTO operation to be applied to a
 * {@link UpdateableDataContext}. Instead of providing a custom implementation
 * of the {@link UpdateScript} interface, one can use this pre-built
 * single-record insertion implementation. Some {@link DataContext}s may even
 * optimize specifically based on the knowledge that there will only be a single
 * record inserted.
 */
public final class InsertInto extends AbstractRowBuilder<InsertInto> implements UpdateScript, RowBuilder<InsertInto> {

    private final Table _table;

    public InsertInto(Table table) {
        super(table);
        _table = table;
    }

    @Override
    public void run(UpdateCallback callback) {
        RowInsertionBuilder insertBuilder = callback.insertInto(getTable());

        final Column[] columns = getColumns();
        final Object[] values = getValues();
        final Style[] styles = getStyles();
        final boolean[] explicitNulls = getExplicitNulls();

        for (int i = 0; i < columns.length; i++) {
            Object value = values[i];
            Column column = columns[i];
            Style style = styles[i];
            if (value == null) {
                if (explicitNulls[i]) {
                    insertBuilder = insertBuilder.value(column, value, style);
                }
            } else {
                insertBuilder = insertBuilder.value(column, value, style);
            }
        }

        insertBuilder.execute();
    }

    @Override
    public Table getTable() {
        return _table;
    }
}
