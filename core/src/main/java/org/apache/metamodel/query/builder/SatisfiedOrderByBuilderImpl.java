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
package org.apache.metamodel.query.builder;

import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.OrderByItem;
import org.apache.metamodel.query.OrderByItem.Direction;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;

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