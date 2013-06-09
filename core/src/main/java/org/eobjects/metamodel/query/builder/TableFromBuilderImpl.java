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

import java.util.List;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.query.JoinType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Table;

final class TableFromBuilderImpl extends SatisfiedFromBuilderCallback implements
		TableFromBuilder {

	private FromItem fromItem;

	public TableFromBuilderImpl(Table table, Query query,
			DataContext dataContext) {
		super(query, dataContext);

		fromItem = new FromItem(table);
		query.from(fromItem);
	}

	@Override
	public JoinFromBuilder innerJoin(Table table) {
		if (table == null) {
			throw new IllegalArgumentException("table cannot be null");
		}
		return new JoinFromBuilderImpl(getQuery(), fromItem, table,
				JoinType.INNER, getDataContext());
	}

	@Override
	public JoinFromBuilder leftJoin(Table table) {
		if (table == null) {
			throw new IllegalArgumentException("table cannot be null");
		}
		return new JoinFromBuilderImpl(getQuery(), fromItem, table,
				JoinType.LEFT, getDataContext());
	}

	@Override
	public JoinFromBuilder rightJoin(Table table) {
		if (table == null) {
			throw new IllegalArgumentException("table cannot be null");
		}
		return new JoinFromBuilderImpl(getQuery(), fromItem, table,
				JoinType.RIGHT, getDataContext());
	}

	@Override
	public TableFromBuilder as(String alias) {
		if (alias == null) {
			throw new IllegalArgumentException("alias cannot be null");
		}
		fromItem.setAlias(alias);
		return this;
	}

	@Override
	protected void decorateIdentity(List<Object> identifiers) {
		super.decorateIdentity(identifiers);
		identifiers.add(fromItem);
	}
}