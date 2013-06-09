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
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;

final class JoinFromBuilderImpl extends SatisfiedFromBuilderCallback implements
		JoinFromBuilder {

	private JoinType joinType;
	private FromItem leftItem;
	private FromItem rightItem;

	public JoinFromBuilderImpl(Query query, FromItem leftItem,
			Table rightTable, JoinType joinType, DataContext dataContext) {
		super(query, dataContext);
		this.joinType = joinType;
		this.leftItem = leftItem;
		this.rightItem = new FromItem(rightTable);
	}

	@Override
	public SatisfiedFromBuilder on(Column left, Column right) {
		if (left == null) {
			throw new IllegalArgumentException("left cannot be null");
		}
		if (right == null) {
			throw new IllegalArgumentException("right cannot be null");
		}
		getQuery().getFromClause().removeItem(leftItem);

		SelectItem[] leftOn = new SelectItem[] { new SelectItem(left) };
		SelectItem[] rightOn = new SelectItem[] { new SelectItem(right) };
		FromItem fromItem = new FromItem(joinType, leftItem, rightItem, leftOn,
				rightOn);

		getQuery().from(fromItem);

		return this;
	}
	
	@Override
	protected void decorateIdentity(List<Object> identifiers) {
		super.decorateIdentity(identifiers);
		identifiers.add(joinType);
		identifiers.add(leftItem);
		identifiers.add(rightItem);
	}
}