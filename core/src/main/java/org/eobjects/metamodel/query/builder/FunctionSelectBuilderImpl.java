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

import org.eobjects.metamodel.query.FunctionType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;

final class FunctionSelectBuilderImpl extends SatisfiedSelectBuilderImpl
		implements FunctionSelectBuilder<GroupedQueryBuilder> {

	private SelectItem selectItem;

	public FunctionSelectBuilderImpl(FunctionType functionType, Column column,
			Query query, GroupedQueryBuilder queryBuilder) {
		super(queryBuilder);

		this.selectItem = new SelectItem(functionType, column);

		query.select(selectItem);
	}

	@Override
	public GroupedQueryBuilder as(String alias) {
		if (alias == null) {
			throw new IllegalArgumentException("alias cannot be null");
		}
		selectItem.setAlias(alias);
		return getQueryBuilder();
	}

	@Override
	protected void decorateIdentity(List<Object> identifiers) {
		super.decorateIdentity(identifiers);
		identifiers.add(selectItem);
	}
}