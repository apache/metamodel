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

import java.io.Serializable;

import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;

/**
 * Represents a row of data in a DataSet. Each row is a mapping between
 * SelectItems and values for each SelectItem.
 * 
 * @see DataSet
 * @see SelectItem
 */
public interface Row extends Serializable {

    /**
     * Gets the value of the provided SelectItem.
     * 
     * @param item
     * @return the value that corresponds to the provided SelectItem. Can be
     *         null if either the value <i>is</i> null or if no value exists
     *         that matches the SelectItem.
     */
    public Object getValue(SelectItem item);

    /**
     * Shorthand method for getting the value of a SelectItem based on the
     * provided column. Invoking this method is equivalent to invoking
     * getValue(new SelectItem(column)).
     * 
     * @param column
     * @return the value of the specified column
     */
    public Object getValue(Column column);

    /**
     * Gets the value of the row at a given index
     * 
     * @param index
     * @return the value at the specified index
     * @throws IndexOutOfBoundsException
     *             if the provided index is out of range
     */
    public Object getValue(int index) throws IndexOutOfBoundsException;

    public Style getStyle(SelectItem item);

    public Style getStyle(Column column);

    public Style getStyle(int index) throws IndexOutOfBoundsException;
    
    public Style[] getStyles();

    /**
     * Gets the index of a SelectItem in the row.
     * 
     * @param item
     *            the item to get the index of
     * @return the index of a SelectItem in the row. If the SelectItem is not
     *         found -1 will be returned.
     */
    public int indexOf(SelectItem item);

    /**
     * Gets the index of a Column in the row.
     * 
     * @param column
     *            the column to get the index of
     * @return the index of a column in the row. If the Column is not found, -1
     *         will be returned.
     */
    public int indexOf(Column column);

    /**
     * Gets the select items that represent the columns of the {@link DataSet}
     * that this row pertains to.
     * 
     * @return
     */
    public SelectItem[] getSelectItems();

    /**
     * Gets the values of the row, represented as an object array
     * 
     * @return an array of objects, containing the values of this row.
     */
    public Object[] getValues();

    /**
     * Creates a row similar to this one but only with a subset of the values.
     * 
     * @param selectItems
     *            the select items (~ columns) to sub-select the row with
     * @return a new Row object containing only the select items requested
     * @deprecated use {@link #getSubSelection(DataSetHeader)} instead.
     */
    @Deprecated
    public Row getSubSelection(SelectItem[] selectItems);

    /**
     * Creates a row similar to this one but only with a subset of the values.
     * 
     * @param header
     *            the new header to sub-select the row with
     * @return a new Row object containing only the select items in the newly
     *         requested header
     */
    public Row getSubSelection(DataSetHeader header);

    /**
     * Gets the amount of values/columns/select items represented in this row.
     * 
     * @return
     */
    public int size();
}