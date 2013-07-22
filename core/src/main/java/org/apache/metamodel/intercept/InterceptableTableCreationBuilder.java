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
package org.apache.metamodel.intercept;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.create.ColumnCreationBuilder;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.schema.Table;

final class InterceptableTableCreationBuilder implements TableCreationBuilder {

	private final TableCreationBuilder _tabelCreationBuilder;
	private final InterceptorList<TableCreationBuilder> _tableCreationInterceptors;

	public InterceptableTableCreationBuilder(
			TableCreationBuilder tabelCreationBuilder,
			InterceptorList<TableCreationBuilder> tableCreationInterceptors) {
		_tabelCreationBuilder = tabelCreationBuilder;
		_tableCreationInterceptors = tableCreationInterceptors;
	}
	
	@Override
	public String toSql() {
	    return _tabelCreationBuilder.toSql();
	}

	@Override
	public TableCreationBuilder like(Table table) {
		_tabelCreationBuilder.like(table);
		return this;
	}

	@Override
	public ColumnCreationBuilder withColumn(String name) {
		ColumnCreationBuilder columnCreationBuilder = _tabelCreationBuilder
				.withColumn(name);
		return new InterceptableColumnCreationBuilder(columnCreationBuilder,
				this);
	}

	@Override
	public Table toTable() {
		return _tabelCreationBuilder.toTable();
	}

	@Override
	public Table execute() throws MetaModelException {
		TableCreationBuilder tableCreationBuilder = _tabelCreationBuilder;
		tableCreationBuilder = _tableCreationInterceptors
				.interceptAll(tableCreationBuilder);
		return tableCreationBuilder.execute();
	}
}
