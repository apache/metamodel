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
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

final class InterceptableRowInsertionBuilder implements RowInsertionBuilder {

	private final RowInsertionBuilder _rowInsertionBuilder;
	private final InterceptorList<RowInsertionBuilder> _rowInsertionInterceptors;

	public InterceptableRowInsertionBuilder(
			RowInsertionBuilder rowInsertionBuilder,
			InterceptorList<RowInsertionBuilder> rowInsertionInterceptors) {
		_rowInsertionBuilder = rowInsertionBuilder;
		_rowInsertionInterceptors = rowInsertionInterceptors;
	}
	
	@Override
	public String toSql() {
	    return _rowInsertionBuilder.toSql();
	}

	@Override
	public RowInsertionBuilder value(int columnIndex, Object value) {
		_rowInsertionBuilder.value(columnIndex, value);
		return this;
	}

	@Override
	public RowInsertionBuilder value(int columnIndex, Object value, Style style) {
		_rowInsertionBuilder.value(columnIndex, value, style);
		return this;
	}

	@Override
	public RowInsertionBuilder value(Column column, Object value) {
		_rowInsertionBuilder.value(column, value);
		return this;
	}

	@Override
	public RowInsertionBuilder value(Column column, Object value, Style style) {
		_rowInsertionBuilder.value(column, value, style);
		return this;
	}

	@Override
	public RowInsertionBuilder value(String columnName, Object value) {
		_rowInsertionBuilder.value(columnName, value);
		return this;
	}
	
    @Override
    public RowInsertionBuilder like(Row row) {
        _rowInsertionBuilder.like(row);
        return this;
    }

	@Override
	public RowInsertionBuilder value(String columnName, Object value,
			Style style) {
		_rowInsertionBuilder.value(columnName, value, style);
		return this;
	}

	@Override
	public void execute() throws MetaModelException {
		RowInsertionBuilder rowInsertionBuilder = _rowInsertionBuilder;
		rowInsertionBuilder = _rowInsertionInterceptors
				.interceptAll(rowInsertionBuilder);
		rowInsertionBuilder.execute();
	}

	@Override
	public Row toRow() {
		return _rowInsertionBuilder.toRow();
	}

	@Override
	public Table getTable() {
		return _rowInsertionBuilder.getTable();
	}

	@Override
	public boolean isSet(Column column) {
		return _rowInsertionBuilder.isSet(column);
	}

}
