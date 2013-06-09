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

import java.io.Serializable;

import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;

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