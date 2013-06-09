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

import java.util.Arrays;

import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;

/**
 * An abstract row that decorates another row. Useful for virtual data that may
 * e.g. be converting physical data etc.
 */
public abstract class AbstractRow implements Cloneable, Row {

    private static final long serialVersionUID = 1L;

    protected abstract DataSetHeader getHeader();

    @Override
    public final int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(getValues());
        return result;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Row other = (Row) obj;
        if (!Arrays.equals(getValues(), other.getValues()))
            return false;
        return true;
    }

    @Override
    public final String toString() {
        return "Row[values=" + Arrays.toString(getValues()) + "]";
    }

    @Override
    public final Object getValue(SelectItem item) {
        int index = indexOf(item);
        if (index == -1) {
            return null;
        }
        return getValue(index);
    }

    @Override
    public final Style getStyle(SelectItem item) {
        int index = indexOf(item);
        if (index == -1) {
            return Style.NO_STYLE;
        }
        return getStyle(index);
    }

    @Override
    public final Style getStyle(Column column) {
        int index = indexOf(column);
        if (index == -1) {
            return Style.NO_STYLE;
        }
        return getStyle(index);
    }

    @Override
    public Object[] getValues() {
        final Object[] values = new Object[size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = getValue(i);
        }
        return values;
    }

    @Override
    public final Object getValue(Column column) {
        int index = indexOf(column);
        if (index == -1) {
            return null;
        }
        return getValue(index);
    }

    @Override
    public final int indexOf(SelectItem item) {
        if (item == null) {
            return -1;
        }
        return getHeader().indexOf(item);
    }

    @Override
    public final int indexOf(Column column) {
        if (column == null) {
            return -1;
        }
        return getHeader().indexOf(column);
    }

    @Override
    public Row getSubSelection(final SelectItem[] selectItems) {
        final DataSetHeader header = new SimpleDataSetHeader(selectItems);
        return getSubSelection(header);
    }

    @Override
    public final SelectItem[] getSelectItems() {
        return getHeader().getSelectItems();
    }

    @Override
    public final int size() {
        return getHeader().size();
    }

    @Override
    public Style[] getStyles() {
        final Style[] styles = new Style[size()];
        for (int i = 0; i < styles.length; i++) {
            styles[i] = getStyle(i);
        }
        return styles;
    }

    @Override
    protected Row clone() {
        return new DefaultRow(getHeader(), getValues(), getStyles());
    }

    @Override
    public final Row getSubSelection(DataSetHeader header) {
        final int size = header.size();
        final Object[] values = new Object[size];
        final Style[] styles = new Style[size];
        for (int i = 0; i < size; i++) {
            final SelectItem selectItem = header.getSelectItem(i);

            if (selectItem.getSubQuerySelectItem() != null) {
                values[i] = getValue(selectItem.getSubQuerySelectItem());
                styles[i] = getStyle(selectItem.getSubQuerySelectItem());
                if (values[i] == null) {
                    values[i] = getValue(selectItem);
                    styles[i] = getStyle(selectItem);
                }
            } else {
                values[i] = getValue(selectItem);
                styles[i] = getStyle(selectItem);
            }
        }
        return new DefaultRow(header, values, styles);
    }
}
