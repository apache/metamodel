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

import java.util.List;

import org.apache.metamodel.schema.Column;

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