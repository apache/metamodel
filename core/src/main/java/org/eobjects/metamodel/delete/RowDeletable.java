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
package org.eobjects.metamodel.delete;

import org.eobjects.metamodel.schema.Table;

public interface RowDeletable {

    /**
     * Determines whether row delete is supported
     * 
     * @return true if row delete is supported
     */
    public boolean isDeleteSupported();

    /**
     * Initiates a row deletion builder.
     * 
     * @param table
     * @return
     * @throws IllegalArgumentException
     * @throws IllegalStateException
     * @throws UnsupportedOperationException
     */
    public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException;

    /**
     * Initiates a row deletion builder.
     * 
     * @param tableName
     * @return
     * @throws IllegalArgumentException
     * @throws IllegalStateException
     * @throws UnsupportedOperationException
     */
    public RowDeletionBuilder deleteFrom(String tableName) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException;

    /**
     * Initiates a row deletion builder.
     * 
     * @param schemaName
     * @param tableName
     * @return
     * @throws IllegalArgumentException
     * @throws IllegalStateException
     * @throws UnsupportedOperationException
     */
    public RowDeletionBuilder deleteFrom(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException, UnsupportedOperationException;
}
