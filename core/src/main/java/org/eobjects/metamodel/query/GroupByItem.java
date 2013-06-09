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

import java.util.List;

import org.eobjects.metamodel.util.BaseObject;

/**
 * Represents a GROUP BY item. GroupByItems always use a select item (that may
 * or not be a part of the query already) for grouping.
 * 
 * @see GroupByClause
 */
public class GroupByItem extends BaseObject implements QueryItem, Cloneable {

    private static final long serialVersionUID = 5218878395877852919L;
    private final SelectItem _selectItem;
    private Query _query;

    /**
     * Constructs a GROUP BY item based on a select item that should be grouped.
     * 
     * @param selectItem
     */
    public GroupByItem(SelectItem selectItem) {
        if (selectItem == null) {
            throw new IllegalArgumentException("SelectItem cannot be null");
        }
        _selectItem = selectItem;
    }

    public SelectItem getSelectItem() {
        return _selectItem;
    }

    @Override
    public String toSql() {
        return toSql(false);
    }

    @Override
    public String toSql(boolean includeSchemaInColumnPaths) {
        final String sameQueryAlias = _selectItem.getSameQueryAlias(includeSchemaInColumnPaths);
        return sameQueryAlias;
    }

    @Override
    public String toString() {
        return toSql();
    }

    public Query getQuery() {
        return _query;
    }

    public GroupByItem setQuery(Query query) {
        _query = query;
        if (_selectItem != null) {
            _selectItem.setQuery(query);
        }
        return this;
    }

    @Override
    protected GroupByItem clone() {
        GroupByItem g = new GroupByItem(_selectItem.clone());
        return g;
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(_selectItem);
    }
}