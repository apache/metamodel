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

import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;

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
    public final List<SelectItem> getSelectItems() {
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
