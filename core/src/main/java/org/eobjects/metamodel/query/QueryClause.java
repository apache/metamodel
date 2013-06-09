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

import java.io.Serializable;
import java.util.List;

public interface QueryClause<E> extends Serializable {

	public QueryClause<E> setItems(E... items);

	public QueryClause<E> addItems(E... items);

	public QueryClause<E> addItems(Iterable<E> items);

	public QueryClause<E> addItem(int index, E item);
	
	public QueryClause<E> addItem(E item);
	
	public boolean isEmpty();

	public int getItemCount();

	public E getItem(int index);

	public List<E> getItems();

	public QueryClause<E> removeItem(int index);

	public QueryClause<E> removeItem(E item);

	public QueryClause<E> removeItems();
	
	public String toSql(boolean includeSchemaInColumnPaths);

	public String toSql();
}
