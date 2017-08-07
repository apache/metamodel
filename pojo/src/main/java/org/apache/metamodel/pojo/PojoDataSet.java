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
package org.apache.metamodel.pojo;

import java.util.Iterator;
import java.util.List;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;

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

    public PojoDataSet(TableDataProvider<E> pojoTable, List<SelectItem> selectItems) {
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
