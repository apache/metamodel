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

/**
 * Default Row implementation. Holds values in memory.
 * 
 * @author Kasper Sørensen
 */
public final class DefaultRow extends AbstractRow implements Row {

    private static final long serialVersionUID = 1L;

    private final DataSetHeader _header;
    private final Object[] _values;
    private final Style[] _styles;
    
    /**
     * This field was replaced by the DataSetHeader field above.
     * 
     * @deprecated no longer used, except for in deserialized objects
     */
    @Deprecated
    private List<SelectItem> _items;

    /**
     * Constructs a row.
     * 
     * @param header
     * @param values
     * @param styles
     */
    public DefaultRow(DataSetHeader header, Object[] values, Style[] styles) {
        if (header == null) {
            throw new IllegalArgumentException("DataSet header cannot be null");
        }
        if (values == null) {
            throw new IllegalArgumentException("Values cannot be null");
        }
        if (header.size() != values.length) {
            throw new IllegalArgumentException("Header size and values length must be equal. " + header.size()
                    + " select items present in header and encountered these values: " + Arrays.toString(values));
        }
        if (styles != null) {
            if (values.length != styles.length) {
                throw new IllegalArgumentException("Values length and styles length must be equal. " + values.length
                        + " values present and encountered these styles: " + Arrays.toString(styles));
            }
            boolean entirelyNoStyle = true;
            for (int i = 0; i < styles.length; i++) {
                if (styles[i] == null) {
                    throw new IllegalArgumentException("Elements in the style array cannot be null");
                }
                if (entirelyNoStyle && !Style.NO_STYLE.equals(styles[i])) {
                    entirelyNoStyle = false;
                }
            }

            if (entirelyNoStyle) {
                // no need to reference any styles
                styles = null;
            }
        }
        _header = header;
        _values = values;
        _styles = styles;
    }

    /**
     * Constructs a row.
     * 
     * @param header
     * @param values
     */
    public DefaultRow(DataSetHeader header, Object[] values) {
        this(header, values, null);
    }

    /**
     * Constructs a row from an array of SelectItems and an array of
     * corresponding values
     * 
     * @param items
     *            the array of SelectItems
     * @param values
     *            the array of values
     * 
     * @deprecated use {@link #DefaultRow(DataSetHeader, Object[])} or
     *             {@link #DefaultRow(DataSetHeader, Object[], Style[])}
     *             instead.
     */
    @Deprecated
    public DefaultRow(SelectItem[] items, Object[] values) {
        this(Arrays.asList(items), values, null);
    }

    /**
     * Constructs a row from an array of SelectItems and an array of
     * corresponding values
     * 
     * @param items
     *            the array of SelectItems
     * @param values
     *            the array of values
     * @param styles
     *            an optional array of styles
     * @deprecated use {@link #DefaultRow(DataSetHeader, Object[])} or
     *             {@link #DefaultRow(DataSetHeader, Object[], Style[])}
     *             instead.
     */
    @Deprecated
    public DefaultRow(SelectItem[] items, Object[] values, Style[] styles) {
        this(Arrays.asList(items), values, styles);
    }

    /**
     * Constructs a row from a list of SelectItems and an array of corresponding
     * values
     * 
     * @param items
     *            the list of SelectItems
     * @param values
     *            the array of values
     * @deprecated use {@link #DefaultRow(DataSetHeader, Object[])} or
     *             {@link #DefaultRow(DataSetHeader, Object[], Style[])}
     *             instead.
     */
    @Deprecated
    public DefaultRow(List<SelectItem> items, Object[] values) {
        this(items, values, null);
    }

    /**
     * Constructs a row from a list of SelectItems and an array of corresponding
     * values
     * 
     * @param items
     *            the list of SelectItems
     * @param values
     *            the array of values
     * @param styles
     *            an optional array of styles
     * @deprecated use {@link #DefaultRow(DataSetHeader, Object[])} or
     *             {@link #DefaultRow(DataSetHeader, Object[], Style[])}
     *             instead.
     */
    @Deprecated
    public DefaultRow(List<SelectItem> items, Object[] values, Style[] styles) {
        this(new SimpleDataSetHeader(items), values, styles);
    }

    @Override
    public Object getValue(int index) throws ArrayIndexOutOfBoundsException {
        return _values[index];
    }

    @Override
    public Object[] getValues() {
        return _values;
    }

    @Override
    public Style getStyle(int index) throws IndexOutOfBoundsException {
        if (_styles == null) {
            return Style.NO_STYLE;
        }
        return _styles[index];
    }
    
    @Override
    public Style[] getStyles() {
        return _styles;
    }

    @Override
    protected DataSetHeader getHeader() {
        if (_header == null && _items != null) {
            // this only happens for deserialized objects which where serialized
            // prior to the introduction of DataSetHeader.
            return new SimpleDataSetHeader(_items);
        }
        return _header;
    }
}