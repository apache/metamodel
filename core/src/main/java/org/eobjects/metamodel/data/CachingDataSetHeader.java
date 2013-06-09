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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;

/**
 * Most common implementation of {@link DataSetHeader}. This implementation is
 * 'caching' in the sense that index values of selectitems are cached in a map
 * to provide quick access when looking up by {@link SelectItem} or
 * {@link Column}.
 */
public final class CachingDataSetHeader extends SimpleDataSetHeader implements DataSetHeader {

    private static final long serialVersionUID = 1L;

    // map of select item identity codes and indexes in the dataset
    private transient Map<Integer, Integer> _selectItemIndexCache;

    // map of column identity codes and indexes in the dataset
    private transient Map<Integer, Integer> _columnIndexCache;

    public CachingDataSetHeader(List<SelectItem> items) {
        super(items);
    }

    public CachingDataSetHeader(SelectItem[] items) {
        this(Arrays.asList(items));
    }

    @Override
    public int indexOf(Column column) {
        if (column == null) {
            return -1;
        }

        if (_columnIndexCache == null) {
            _columnIndexCache = new ConcurrentHashMap<Integer, Integer>(super.size());
        }

        final int identityCode = System.identityHashCode(column);
        Integer index = _columnIndexCache.get(identityCode);
        if (index == null) {
            index = super.indexOf(column);

            if (index != -1) {
                _columnIndexCache.put(identityCode, index);
            }
        }
        return index;
    }

    @Override
    public final int indexOf(SelectItem item) {
        if (item == null) {
            return -1;
        }

        if (_selectItemIndexCache == null) {
            _selectItemIndexCache = new ConcurrentHashMap<Integer, Integer>(super.size());
        }

        final int identityCode = System.identityHashCode(item);
        Integer index = _selectItemIndexCache.get(identityCode);
        if (index == null) {
            index = super.indexOf(item);

            if (index != -1) {
                _selectItemIndexCache.put(identityCode, index);
            }
        }
        return index;
    }
}
