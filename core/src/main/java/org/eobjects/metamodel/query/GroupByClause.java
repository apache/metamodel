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
 * Represents the GROUP BY clause of a query that contains GroupByItem's.
 * 
 * @see GroupByItem
 */
public class GroupByClause extends AbstractQueryClause<GroupByItem> {

	private static final long serialVersionUID = -3824934110331202101L;

	public GroupByClause(Query query) {
		super(query, AbstractQueryClause.PREFIX_GROUP_BY,
				AbstractQueryClause.DELIM_COMMA);
	}

	public List<SelectItem> getEvaluatedSelectItems() {
		final List<SelectItem> result = new ArrayList<SelectItem>();
		final List<GroupByItem> items = getItems();
		for (GroupByItem item : items) {
			result.add(item.getSelectItem());
		}
		return result;
	}

}