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

/**
 * Represents the ORDER BY clause of a query containing OrderByItem's. The order
 * and direction of the OrderItems define the way that the result of a query
 * will be sorted.
 * 
 * @see OrderByItem
 */
public class OrderByClause extends AbstractQueryClause<OrderByItem> {

	private static final long serialVersionUID = 2441926135870143715L;

	public OrderByClause(Query query) {
		super(query, AbstractQueryClause.PREFIX_ORDER_BY,
				AbstractQueryClause.DELIM_COMMA);
	}

	public List<SelectItem> getEvaluatedSelectItems() {
		final List<SelectItem> result = new ArrayList<SelectItem>();
		final List<OrderByItem> items = getItems();
		for (OrderByItem item : items) {
			result.add(item.getSelectItem());
		}
		return result;
	}

}