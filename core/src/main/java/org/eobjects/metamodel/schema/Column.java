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
package org.eobjects.metamodel.schema;

import java.io.Serializable;

/**
 * Represents a column and it's metadata description. Columns reside within a
 * Table and can be used as keys for relationships between tables.
 * 
 * @see Table
 * 
 * @author Kasper Sørensen
 */
public interface Column extends Comparable<Column>, Serializable, NamedStructure {

    /**
     * Gets the name of this Column
     * 
     * @return the name of this Column
     */
    @Override
    public String getName();

    /**
     * Returns the column number or index. Note: This column number is 0-based
     * whereas the JDBC is 1-based.
     * 
     * @return the number of this column.
     */
    public int getColumnNumber();

    /**
     * Gets the type of the column
     * 
     * @return this column's type.
     */
    public ColumnType getType();

    /**
     * Gets the table for which this column belong
     * 
     * @return this column's table.
     */
    public Table getTable();

    /**
     * Determines whether or not this column accepts null values.
     * 
     * @return true if this column accepts null values, false if not and null if
     *         not known.
     */
    public Boolean isNullable();

    /**
     * Gets any remarks/comments to this column.
     * 
     * @return any remarks/comments to this column.
     */
    public String getRemarks();

    /**
     * Gets the data type size of this column.
     * 
     * @return the data type size of this column or null if the size is not
     *         determined or known.
     */
    public Integer getColumnSize();

    /**
     * Gets the native type of this column. A native type is the name of the
     * data type as defined in the datastore.
     * 
     * @return the name of the native type.
     */
    public String getNativeType();

    /**
     * Determines if this column is indexed.
     * 
     * @return true if this column is indexed or false if not (or not known)
     */
    public boolean isIndexed();

    /**
     * Determines if this column is (one of) the primary key(s) of its table.
     * 
     * @return true if this column is a primary key, or false if not (or if this
     *         is not determinable).
     */
    public boolean isPrimaryKey();
}