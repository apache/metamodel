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
package org.eobjects.metamodel.insert;

import org.eobjects.metamodel.schema.Table;

/**
 * An interface for objects that support inserting rows into tables.
 * 
 * @author Kasper Sørensen
 */
public interface RowInsertable {

    /**
     * Determines whether row insertion is supported
     * 
     * @return true if row insertion is supported
     */
    public boolean isInsertSupported();

    /**
     * Initiates the building of a row insertion operation.
     * 
     * @param table
     *            the table to insert a row into
     * @return a builder object on which values can be added and the statement
     *         can be committed.
     * @throws IllegalArgumentException
     *             if the table argument is null or invalid.
     * @throws IllegalStateException
     *             if the connection to the DataContext is read-only or another
     *             access restriction is preventing the operation.
     * @throws UnsupportedOperationException
     *             in case {@link #isInsertSupported()} is false
     */
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException;

    /**
     * Initiates the building of a row insertion operation.
     * 
     * @param tableName
     *            the name of the table to insert a row into
     * @return a builder object on which values can be added and the statement
     *         can be committed.
     * @throws IllegalArgumentException
     *             if the tableName argument is null or invalid.
     * @throws IllegalStateException
     *             if the connection to the DataContext is read-only or another
     *             access restriction is preventing the operation.
     * @throws UnsupportedOperationException
     *             in case {@link #isInsertSupported()} is false
     */
    public RowInsertionBuilder insertInto(String tableName) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException;

    /**
     * Initiates the building of a row insertion operation.
     * 
     * @param schemaName
     *            the name of the schema
     * @param tableName
     *            the name of the table to insert a row into
     * @return a builder object on which values can be added and the statement
     *         can be committed.
     * @throws IllegalArgumentException
     *             if the tableName argument is null or invalid.
     * @throws IllegalStateException
     *             if the connection to the DataContext is read-only or another
     *             access restriction is preventing the operation.
     * @throws UnsupportedOperationException
     *             in case {@link #isInsertSupported()} is false
     */
    public RowInsertionBuilder insertInto(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException, UnsupportedOperationException;
}
