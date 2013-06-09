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
 * Represents an ORDER BY item. An OrderByItem sorts the resulting DataSet
 * according to a SelectItem that may or may not be a part of the query already.
 * 
 * @see OrderByClause
 * @see SelectItem
 */
public class OrderByItem extends BaseObject implements QueryItem, Cloneable {

	public enum Direction {
		ASC, DESC
	}

	private static final long serialVersionUID = -8397473619828484774L;
	private final SelectItem _selectItem;
	private Direction _direction;
	private Query _query;

	/**
	 * Creates an OrderByItem
	 * 
	 * @param selectItem
	 *            the select item to order
	 * @param direction
	 *            the direction to order the select item
	 */
	public OrderByItem(SelectItem selectItem, Direction direction) {
		if (selectItem == null) {
			throw new IllegalArgumentException("SelectItem cannot be null");
		}
		_selectItem = selectItem;
		_direction = direction;
	}

	/**
	 * Creates an OrderByItem
	 * 
	 * @param selectItem
	 * @param ascending
	 * @deprecated user OrderByItem(SelectItem, Direction) instead
	 */
	@Deprecated
	public OrderByItem(SelectItem selectItem, boolean ascending) {
		if (selectItem == null) {
			throw new IllegalArgumentException("SelectItem cannot be null");
		}
		_selectItem = selectItem;
		if (ascending) {
			_direction = Direction.ASC;
		} else {
			_direction = Direction.DESC;
		}
	}

	/**
	 * Creates an ascending OrderByItem
	 * 
	 * @param selectItem
	 */
	public OrderByItem(SelectItem selectItem) {
		this(selectItem, Direction.ASC);
	}
	

    @Override
    public String toSql(boolean includeSchemaInColumnPaths) {
        StringBuilder sb = new StringBuilder();
        sb.append(_selectItem.getSameQueryAlias(includeSchemaInColumnPaths) + ' ');
        sb.append(_direction);
        return sb.toString();
    }

	@Override
	public String toSql() {
	    return toSql(false);
	}

	public boolean isAscending() {
		return (_direction == Direction.ASC);
	}

	public boolean isDescending() {
		return (_direction == Direction.DESC);
	}

	public Direction getDirection() {
		return _direction;
	}

	public OrderByItem setDirection(Direction direction) {
		_direction = direction;
		return this;
	}

	public SelectItem getSelectItem() {
		return _selectItem;
	}

	public Query getQuery() {
		return _query;
	}

	public OrderByItem setQuery(Query query) {
		_query = query;
		if (_selectItem != null) {
			_selectItem.setQuery(query);
		}
		return this;
	}

	@Override
	protected OrderByItem clone() {
		OrderByItem o = new OrderByItem(_selectItem.clone());
		o._direction = _direction;
		return o;
	}

	@Override
	protected void decorateIdentity(List<Object> identifiers) {
		identifiers.add(_direction);
		identifiers.add(_selectItem);
	}

	@Override
	public String toString() {
		return toSql();
	}
}