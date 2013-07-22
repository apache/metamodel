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

import org.apache.metamodel.insert.AbstractRowInsertionBuilder;
import org.apache.metamodel.schema.Table;

final class CsvInsertBuilder extends AbstractRowInsertionBuilder<CsvUpdateCallback> {

	public CsvInsertBuilder(CsvUpdateCallback updateCallback, Table table) {
		super(updateCallback, table);
	}

	@Override
	public void execute() {
		Object[] values = getValues();
		String[] stringValues = new String[values.length];
		for (int i = 0; i < stringValues.length; i++) {
			stringValues[i] = values[i] == null ? "" : values[i].toString();
		}
		getUpdateCallback().writeRow(stringValues, true);
	}

}
