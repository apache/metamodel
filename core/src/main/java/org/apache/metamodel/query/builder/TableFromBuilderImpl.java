/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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