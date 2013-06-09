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
import java.util.List;

import org.eobjects.metamodel.MetaModelHelper;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;

/**
 * Simple implementation of {@link DataSetHeader} which does no magic to improve
 * performance.
 * 
 * Note that except for datasets with very few records, the
 * {@link CachingDataSetHeader} is preferred.
 */
public class SimpleDataSetHeader implements DataSetHeader {

    private static final long serialVersionUID = 1L;
    
    private final List<SelectItem> _items;

    public SimpleDataSetHeader(List<SelectItem> items) {
        _items = items;
    }

    public SimpleDataSetHeader(SelectItem[] selectItems) {
        this(Arrays.asList(selectItems));
    }

    public SimpleDataSetHeader(Column[] columns) {
        this(MetaModelHelper.createSelectItems(columns));
    }

    @Override
    public final SelectItem[] getSelectItems() {
        return _items.toArray(new SelectItem[_items.size()]);
    }

    @Override
    public final int size() {
        return _items.size();
    }

    @Override
    public SelectItem getSelectItem(int index) {
        return _items.get(index);
    }
    
    @Override
    public int indexOf(Column column) {
        if (column == null) {
            return -1;
        }
        return indexOf(new SelectItem(column));
    }

    @Override
    public int indexOf(SelectItem item) {
        if (item == null) {
            return -1;
        }
        int i = 0;
        for (SelectItem selectItem : _items) {
            if (item == selectItem) {
                return i;
            }
            i++;
        }

        i = 0;
        for (SelectItem selectItem : _items) {
            if (item.equalsIgnoreAlias(selectItem, true)) {
                return i;
            }
            i++;
        }

        i = 0;
        for (SelectItem selectItem : _items) {
            if (item.equalsIgnoreAlias(selectItem)) {
                return i;
            }
            i++;
        }

        return -1;
    }

    @Override
    public final int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_items == null) ? 0 : _items.hashCode());
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
        SimpleDataSetHeader other = (SimpleDataSetHeader) obj;
        if (_items == null) {
            if (other._items != null)
                return false;
        } else if (!_items.equals(other._items))
            return false;
        return true;
    }
}
