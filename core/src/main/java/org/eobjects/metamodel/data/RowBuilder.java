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

import org.eobjects.metamodel.insert.RowInsertionBuilder;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.update.RowUpdationBuilder;

/**
 * Abstract interface for objects that build rows, either for eg. insert or
 * update purposes.
 * 
 * @see RowInsertionBuilder
 * @see RowUpdationBuilder
 * 
 * @param <RB>
 *            the RowBuilder subtype, used for cascading return values
 */
public interface RowBuilder<RB extends RowBuilder<?>> {

    /**
     * Gets the table that this row builder pertains to.
     * 
     * @return the table that this row builder pertains to.
     */
    public Table getTable();

    /**
     * Sets the value of a column, by column index
     * 
     * @param columnIndex
     * @param value
     * @return
     */
    public RB value(int columnIndex, Object value);

    /**
     * Sets the value of a column, by column index
     * 
     * @param columnIndex
     * @param value
     * @param style
     * @return
     */
    public RB value(int columnIndex, Object value, Style style);

    /**
     * Sets the value of a column
     * 
     * @param column
     * @param value
     * @return
     */
    public RB value(Column column, Object value);

    /**
     * Sets the value of a column
     * 
     * @param column
     * @param value
     * @param style
     * @return
     */
    public RB value(Column column, Object value, Style style);

    /**
     * Sets the value of a column, by column name
     * 
     * @param columnName
     * @param value
     * @return
     */
    public RB value(String columnName, Object value);

    /**
     * Sets the value and the style of this value of a column, by column name
     * 
     * @param columnName
     * @param value
     * @param style
     * @return
     */
    public RB value(String columnName, Object value, Style style);

    /**
     * Gets the built record represented as a {@link Row} object.
     * 
     * @return a {@link Row} object as it will appear if committed and queried.
     */
    public Row toRow();

    /**
     * Determines if a column's value has been explicitly specified or not. This
     * can be used to tell explicit NULL values apart from just unspecified
     * values in a statement.
     * 
     * @param column
     *            the column to check
     * @return true if the column's value has been set, or false if not
     */
    public boolean isSet(Column column);
}
