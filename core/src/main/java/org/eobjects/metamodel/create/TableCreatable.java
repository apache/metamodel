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
package org.eobjects.metamodel.create;

import org.eobjects.metamodel.schema.Schema;

/**
 * Interface for objects that support creating new tables.
 * 
 * @author Kasper Sørensen
 */
public interface TableCreatable {

    /**
     * Determines whether table creation is supported
     * 
     * @return true if table creation is supported
     */
    public boolean isCreateTableSupported();

    /**
     * Initiates the building of a table creation operation.
     * 
     * @param schema
     *            the schema to create the table in
     * @param name
     *            the name of the new table
     * @return a builder object on which the details of the table can be
     *         specified and committed.
     * @throws IllegalArgumentException
     *             if the table argument is null or invalid.
     * @throws IllegalStateException
     *             if the connection to the DataContext is read-only or another
     *             access restriction is preventing the operation.
     */
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException;

    /**
     * Initiates the building of a table creation operation.
     * 
     * @param schemaName
     *            the name of the schema to create the table in
     * @param tableName
     *            the name of the new table
     * @return a builder object on which the details of the table can be
     *         specified and committed.
     * @throws IllegalArgumentException
     *             if the table argument is null or invalid.
     * @throws IllegalStateException
     *             if the connection to the DataContext is read-only or another
     *             access restriction is preventing the operation.
     */
    public TableCreationBuilder createTable(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException;

}
