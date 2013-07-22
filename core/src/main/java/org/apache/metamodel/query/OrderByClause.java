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
package org.apache.metamodel.query;

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