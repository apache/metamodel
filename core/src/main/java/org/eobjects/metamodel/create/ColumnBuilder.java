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

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;

/**
 * Abstract interface for components that build columns.
 * 
 * Defines methods for refining particular properties of the column build.
 * 
 * @param <T>
 *            the return type of the builder methods
 */
public interface ColumnBuilder<T extends ColumnBuilder<?>> {

    /**
     * Builds several properties of a column, based on another {@link Column}
     * object as a prototype.
     * 
     * @param column
     *            a prototype for the column being built.
     * @return a builder object for further column creation.
     */
    public T like(Column column);

    /**
     * Defines the {@link ColumnType} of the created column.
     * 
     * @param type
     *            the column type of the created column.
     * @return a builder object for further column creation.
     */
    public T ofType(ColumnType type);

    /**
     * Defines the native type of the created column (useful especially for SQL
     * based {@link DataContext}s).
     * 
     * @param nativeType
     *            the native type of the created column
     * @return a builder object for further column creation.
     */
    public T ofNativeType(String nativeType);

    /**
     * Defines the size of the created column.
     * 
     * @param size
     *            the size of the created column.
     * @return a builder object for further column creation.
     */
    public T ofSize(int size);

    /**
     * Defines if the created column should be nullable or not.
     * 
     * @param nullable
     *            if the created column should be nullable or not.
     * @return a builder object for further column creation.
     */
    public T nullable(boolean nullable);

    /**
     * Defines that the created column should be a primary key
     * 
     * @return a builder object for further column creation.
     */
    public T asPrimaryKey();
}
