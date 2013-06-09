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

import org.eobjects.metamodel.schema.Column;

/**
 * Represents a clause of filters in the query. This type of clause is used for
 * the WHERE and HAVING parts of an SQL query.
 * 
 * Each provided FilterItem will be evaluated with the logical AND operator,
 * which requires that all filters are applied. Alternatively, if you wan't to
 * use an OR operator, then use the appropriate constructor of FilterItem to
 * create a composite filter.
 * 
 * @see FilterItem
 */
public class FilterClause extends AbstractQueryClause<FilterItem> {

	private static final long serialVersionUID = -9077342278766808934L;

	public FilterClause(Query query, String prefix) {
		super(query, prefix, AbstractQueryClause.DELIM_AND);
	}

	public List<SelectItem> getEvaluatedSelectItems() {
		List<SelectItem> result = new ArrayList<SelectItem>();
		List<FilterItem> items = getItems();
		for (FilterItem item : items) {
			addEvaluatedSelectItems(result, item);
		}
		return result;
	}

	private void addEvaluatedSelectItems(List<SelectItem> result,
			FilterItem item) {
		FilterItem[] orItems = item.getChildItems();
		if (orItems != null) {
			for (FilterItem filterItem : orItems) {
				addEvaluatedSelectItems(result, filterItem);
			}
		}
		SelectItem selectItem = item.getSelectItem();
		if (selectItem != null && !result.contains(selectItem)) {
			result.add(selectItem);
		}
		Object operand = item.getOperand();
		if (operand != null && operand instanceof SelectItem
				&& !result.contains(operand)) {
			result.add((SelectItem) operand);
		}
	}

	/**
	 * Traverses the items and evaluates whether or not the given column is
	 * referenced in either of them.
	 * 
	 * @param column
	 * @return true if the column is referenced in the clause or false if not
	 */
	public boolean isColumnReferenced(Column column) {
		for (FilterItem item : getItems()) {
			if (item.isReferenced(column)) {
				return true;
			}
		}
		return false;
	}
}