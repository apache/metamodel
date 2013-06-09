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

import org.eobjects.metamodel.schema.Column;

/**
 * Represents the SELECT clause of a query containing SelectItems.
 * 
 * @see SelectItem
 */
public class SelectClause extends AbstractQueryClause<SelectItem> {

	private static final long serialVersionUID = -2458447191169901181L;
	private boolean _distinct = false;

	public SelectClause(Query query) {
		super(query, AbstractQueryClause.PREFIX_SELECT, AbstractQueryClause.DELIM_COMMA);
	}

	public SelectItem getSelectItem(Column column) {
		if (column != null) {
			for (SelectItem item : getItems()) {
				if (column.equals(item.getColumn())) {
					return item;
				}
			}
		}
		return null;
	}

	@Override
	public String toSql(boolean includeSchemaInColumnPaths) {
		if (getItems().size() == 0) {
			return "";
		}

		final String sql = super.toSql(includeSchemaInColumnPaths);
        StringBuilder sb = new StringBuilder(sql);
		if (_distinct) {
			sb.insert(AbstractQueryClause.PREFIX_SELECT.length(), "DISTINCT ");
		}
		return sb.toString();
	}

	public boolean isDistinct() {
		return _distinct;
	}

	public void setDistinct(boolean distinct) {
		_distinct = distinct;
	}

	@Override
	protected void decorateIdentity(List<Object> identifiers) {
		super.decorateIdentity(identifiers);
		identifiers.add(_distinct);
	}
}