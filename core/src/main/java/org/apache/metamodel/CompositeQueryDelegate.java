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
package org.apache.metamodel;

import java.util.List;
import java.util.function.Function;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

final class CompositeQueryDelegate extends QueryPostprocessDelegate {

	private final Function<Table, DataContext> _dataContextRetrievalFunction;

	public CompositeQueryDelegate(
			Function<Table, DataContext> dataContextRetrievalFunction) {
		_dataContextRetrievalFunction = dataContextRetrievalFunction;
	}

	@Override
	protected DataSet materializeMainSchemaTable(Table table, List<Column> columns,
			int maxRows) {
		// find the appropriate datacontext to execute a simple
		// table materialization query
		final DataContext dc = _dataContextRetrievalFunction.apply(table);
		final Query q = new Query().select(columns).from(table);
		if (maxRows >= 0) {
			q.setMaxRows(maxRows);
		}
		return dc.executeQuery(q);
	}

}
