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
package org.eobjects.metamodel.query.builder;

import org.eobjects.metamodel.query.FunctionType;
import org.eobjects.metamodel.query.OrderByItem;
import org.eobjects.metamodel.query.OrderByItem.Direction;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;

final class SatisfiedOrderByBuilderImpl extends GroupedQueryBuilderCallback
		implements SatisfiedOrderByBuilder<GroupedQueryBuilder> {

	private OrderByItem orderByitem;

	public SatisfiedOrderByBuilderImpl(Column column, Query query,
			GroupedQueryBuilder queryBuilder) {
		super(queryBuilder);
		orderByitem = new OrderByItem(new SelectItem(column));
		query.orderBy(orderByitem);
	}

	public SatisfiedOrderByBuilderImpl(FunctionType function, Column column,
			Query query, GroupedQueryBuilder queryBuilder) {
		super(queryBuilder);
		orderByitem = new OrderByItem(new SelectItem(function, column));
		query.orderBy(orderByitem);
	}

	@Override
	public GroupedQueryBuilder asc() {
		orderByitem.setDirection(Direction.ASC);
		return getQueryBuilder();
	}

	@Override
	public GroupedQueryBuilder desc() {
		orderByitem.setDirection(Direction.DESC);
		return getQueryBuilder();
	}
	
	@Override
	public SatisfiedOrderByBuilder<GroupedQueryBuilder> and(Column column) {
		return getQueryBuilder().orderBy(column);
	}

}