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
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;

final class InterceptableColumnCreationBuilder implements ColumnCreationBuilder {

	private final ColumnCreationBuilder _columnCreationBuilder;
	private final InterceptableTableCreationBuilder _tableCreationBuilder;

	public InterceptableColumnCreationBuilder(
			ColumnCreationBuilder columnCreationBuilder,
			InterceptableTableCreationBuilder tableCreationBuilder) {
		_columnCreationBuilder = columnCreationBuilder;
		_tableCreationBuilder = tableCreationBuilder;
	}
	
	@Override
	public String toSql() {
	    return _tableCreationBuilder.toSql();
	}

	@Override
	public TableCreationBuilder like(Table table) {
		return _tableCreationBuilder.like(table);
	}

	@Override
	public ColumnCreationBuilder withColumn(String name) {
		_columnCreationBuilder.withColumn(name);
		return this;
	}

	@Override
	public Table toTable() {
		return _tableCreationBuilder.toTable();
	}

	@Override
	public Table execute() throws MetaModelException {
		return _tableCreationBuilder.execute();
	}

	@Override
	public ColumnCreationBuilder like(Column column) {
		_columnCreationBuilder.like(column);
		return this;
	}
	
	@Override
	public ColumnCreationBuilder asPrimaryKey() {
        _columnCreationBuilder.asPrimaryKey();
        return this;
	}

	@Override
	public ColumnCreationBuilder ofType(ColumnType type) {
		_columnCreationBuilder.ofType(type);
		return this;
	}

	@Override
	public ColumnCreationBuilder ofNativeType(String nativeType) {
		_columnCreationBuilder.ofNativeType(nativeType);
		return this;
	}

	@Override
	public ColumnCreationBuilder ofSize(int size) {
		_columnCreationBuilder.ofSize(size);
		return this;
	}

	@Override
	public ColumnCreationBuilder nullable(boolean nullable) {
		_columnCreationBuilder.nullable(nullable);
		return this;
	}

}
