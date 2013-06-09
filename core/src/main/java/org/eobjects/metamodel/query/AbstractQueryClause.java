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

import java.util.ArrayList;
import java.util.List;

import org.eobjects.metamodel.util.BaseObject;

/**
 * Represents an abstract clause in a query. Clauses contains IQueryItems and
 * provide basic ways of adding, modifying and removing these.
 * 
 * @param <E>
 *            the type of query item this QueryClause handles
 * 
 * @see Query
 */
public abstract class AbstractQueryClause<E extends QueryItem> extends BaseObject implements QueryClause<E> {

    private static final long serialVersionUID = 3987346267433022231L;

    public static final String PREFIX_SELECT = "SELECT ";
    public static final String PREFIX_FROM = " FROM ";
    public static final String PREFIX_WHERE = " WHERE ";
    public static final String PREFIX_GROUP_BY = " GROUP BY ";
    public static final String PREFIX_HAVING = " HAVING ";
    public static final String PREFIX_ORDER_BY = " ORDER BY ";
    public static final String DELIM_COMMA = ", ";
    public static final String DELIM_AND = " AND ";

    private final Query _query;
    private final List<E> _items = new ArrayList<E>();
    private final String _prefix;
    private final String _delim;

    public AbstractQueryClause(Query query, String prefix, String delim) {
        _query = query;
        _prefix = prefix;
        _delim = delim;
    }

    @Override
    public QueryClause<E> setItems(E... items) {
        _items.clear();
        return addItems(items);
    }

    @Override
    public QueryClause<E> addItems(E... items) {
        for (E item : items) {
            addItem(item);
        }
        return this;
    }

    @Override
    public QueryClause<E> addItems(Iterable<E> items) {
        for (E item : items) {
            addItem(item);
        }
        return this;
    }

    public QueryClause<E> addItem(int index, E item) {
        if (item.getQuery() == null) {
            item.setQuery(_query);
        }
        _items.add(index, item);
        return this;
    };

    @Override
    public QueryClause<E> addItem(E item) {
        return addItem(getItemCount(), item);
    }

    @Override
    public int getItemCount() {
        return _items.size();
    }

    @Override
    public boolean isEmpty() {
        return getItemCount() == 0;
    }

    @Override
    public E getItem(int index) {
        return _items.get(index);
    }

    @Override
    public List<E> getItems() {
        return _items;
    }

    @Override
    public QueryClause<E> removeItem(int index) {
        _items.remove(index);
        return this;
    }

    @Override
    public QueryClause<E> removeItem(E item) {
        _items.remove(item);
        return this;
    }

    @Override
    public QueryClause<E> removeItems() {
        _items.clear();
        return this;
    }

    @Override
    public String toSql() {
        return toSql(false);
    }

    @Override
    public String toSql(boolean includeSchemaInColumnPaths) {
        if (_items.size() == 0) {
            return "";
        }
        final StringBuilder sb = new StringBuilder(_prefix);
        for (int i = 0; i < _items.size(); i++) {
            final E item = _items.get(i);
            if (i != 0) {
                sb.append(_delim);
            }
            final String sql = item.toSql(includeSchemaInColumnPaths);
            sb.append(sql);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(_items);
    }
}