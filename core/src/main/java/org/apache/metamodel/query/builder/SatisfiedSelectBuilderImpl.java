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
import org.apache.metamodel.schema.Column;

class SatisfiedSelectBuilderImpl extends GroupedQueryBuilderCallback implements
		SatisfiedSelectBuilder<GroupedQueryBuilder> {

	public SatisfiedSelectBuilderImpl(GroupedQueryBuilder queryBuilder) {
		super(queryBuilder);
	}

	@Override
	public ColumnSelectBuilder<GroupedQueryBuilder> and(Column column) {
		if (column == null) {
			throw new IllegalArgumentException("column cannot be null");
		}
		return getQueryBuilder().select(column);
	}

	@Override
	public SatisfiedSelectBuilder<GroupedQueryBuilder> and(Column... columns) {
		if (columns == null) {
			throw new IllegalArgumentException("columns cannot be null");
		}
		return getQueryBuilder().select(columns);
	}

	@Override
	public FunctionSelectBuilder<GroupedQueryBuilder> and(
			FunctionType functionType, Column column) {
		if (functionType == null) {
			throw new IllegalArgumentException("functionType cannot be null");
		}
		if (column == null) {
			throw new IllegalArgumentException("column cannot be null");
		}
		return getQueryBuilder().select(functionType, column);
	}

	@Override
	public SatisfiedSelectBuilder<GroupedQueryBuilder> and(String columnName) {
		if (columnName == null) {
			throw new IllegalArgumentException("columnName cannot be null");
		}
		return getQueryBuilder().select(columnName);
	}

}