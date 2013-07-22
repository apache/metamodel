/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.data;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;

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
