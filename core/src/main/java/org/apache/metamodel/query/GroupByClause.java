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