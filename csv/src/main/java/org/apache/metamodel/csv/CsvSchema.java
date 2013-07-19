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
package org.apache.metamodel.csv;

import org.apache.metamodel.schema.AbstractSchema;
import org.apache.metamodel.schema.Table;

final class CsvSchema extends AbstractSchema {

    private static final long serialVersionUID = 1L;

    private final String _name;
	private final transient CsvDataContext _dataContext;
	private CsvTable _table;

	public CsvSchema(String name, CsvDataContext dataContext) {
		super();
		_name = name;
		_dataContext = dataContext;
	}

	protected void setTable(CsvTable table) {
		_table = table;
	}

	@Override
	public String getName() {
		return _name;
	}

	protected CsvDataContext getDataContext() {
		return _dataContext;
	}

	@Override
	public String getQuote() {
		return null;
	}

	@Override
	public Table[] getTables() {
		if (_table == null) {
			return new Table[0];
		}
		return new Table[] { _table };
	}
}
