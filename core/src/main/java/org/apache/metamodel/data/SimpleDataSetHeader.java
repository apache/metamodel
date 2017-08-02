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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;

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


    public static SimpleDataSetHeader fromColumns(List<Column> cols){
        return new SimpleDataSetHeader(cols.stream().map(SelectItem::new).collect(Collectors.toList()));
    }


    @Override
    public final List<SelectItem> getSelectItems() {
        return Collections.unmodifiableList(_items);
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
        
        final boolean scalarFunctionQueried = item.getScalarFunction() != null;
        if (scalarFunctionQueried) {
            final SelectItem itemWithoutFunction = item.replaceFunction(null);
            return indexOf(itemWithoutFunction);
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
    
    @Override
    public String toString() {
        return "DataSetHeader" + _items.toString();
    }
}
