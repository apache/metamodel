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
package org.eobjects.metamodel.query;

import org.eobjects.metamodel.DataContext;

/**
 * Represents a query parameter, in SQL represented with a '?' symbol.
 * Parameters are values in the query that will be defined at execution time,
 * not parsing/preparation time.
 * 
 * @see CompiledQuery
 * @see DataContext#compileQuery(Query) 
 */
public class QueryParameter {

    @Override
    public String toString() {
        return "?";
    }
}
