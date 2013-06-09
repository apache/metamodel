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

import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.builder.FilterBuilder;
import org.eobjects.metamodel.schema.Column;

/**
 * An interface for builder components that formulate a WHERE clause, either for
 * querying, updating, deleting or other purposes.
 * 
 * @param <T>
 *            the return type of the {@link WhereClauseBuilder}s builder methods
 */
public interface WhereClauseBuilder<T> {

    /**
     * Defines a where item to set as a criteria
     * 
     * @param column
     *            a column to apply a criteria for
     * @return a builder object for further building the where item
     */
    public FilterBuilder<T> where(Column column);

    /**
     * Defines a where item to set as a criteria
     * 
     * @param columnName
     *            the name of the colum to which the criteria will be applied
     * @return a builder object for further building the where item
     */
    public FilterBuilder<T> where(String columnName);

    /**
     * Applies where items to set criteria
     * 
     * @param filterItems
     *            the where items to set
     * @return the builder object itself, for further building of the update
     */
    public T where(FilterItem... filterItems);

    /**
     * Applies where items to set criteria
     * 
     * @param filterItems
     *            the where items to set
     * @return the builder object, for further building of the update
     */
    public T where(Iterable<FilterItem> filterItems);
}
