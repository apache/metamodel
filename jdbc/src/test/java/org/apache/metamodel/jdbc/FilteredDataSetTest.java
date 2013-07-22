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
package org.apache.metamodel.jdbc;

import java.sql.Connection;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.FilteredDataSet;
import org.apache.metamodel.data.IRowFilter;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Table;

public class FilteredDataSetTest extends JdbcTestCase {

	public void testStrategy() throws Exception {
		Connection connection = getTestDbConnection();
		DataContext dc = new JdbcDataContext(connection);
		Table customerTable = dc.getDefaultSchema().getTableByName("CUSTOMERS");

		Query q = new Query().from(customerTable).select(
				customerTable.getColumns());
		DataSet dataSet = dc.executeQuery(q);
		IRowFilter filter1 = new IRowFilter() {

			public boolean accept(Row row) {
				// This will only accept five records
				boolean result = ((Integer) row.getValue(0)) > 485;
				System.out.println("Evaluating: " + row);
				System.out.println("Accepting: " + result);
				return result;
			}

		};
		IRowFilter filter2 = new IRowFilter() {
			int count = 0;

			public boolean accept(Row row) {
				count++;
				return (count == 2 || count == 3);
			}

		};
		dataSet = new FilteredDataSet(dataSet, filter1, filter2);
		assertTrue(dataSet.next());
		assertEquals(
				"Row[values=[487, Signal Collectibles Ltd., Taylor, Sue, 4155554312, 2793 Furth Circle, null, Brisbane, CA, 94217, USA, 1165, 60300.0]]",
				dataSet.getRow().toString());
		assertTrue(dataSet.next());
		assertEquals(
				"Row[values=[489, Double Decker Gift Stores, Ltd, Hardy, Thomas, (171) 555-7555, 120 Hanover Sq., null, London, null, WA1 1DP, UK, 1501, 43300.0]]",
				dataSet.getRow().toString());
		assertFalse(dataSet.next());
		
		dataSet.close();
	}
}