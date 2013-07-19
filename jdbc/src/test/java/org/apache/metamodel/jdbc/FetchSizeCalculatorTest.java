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

import junit.framework.TestCase;

import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableTable;

public class FetchSizeCalculatorTest extends TestCase {

	public void testGetFetchSize() throws Exception {
		FetchSizeCalculator calc = new FetchSizeCalculator(8 * 1024 * 1024);
		assertEquals(10922, calc.getFetchSize(new MutableColumn("a",
				ColumnType.VARCHAR).setColumnSize(256), new MutableColumn("b",
				ColumnType.VARCHAR).setColumnSize(128)));

		assertEquals(2048, calc.getFetchSize(new MutableColumn("a",
				ColumnType.BLOB).setColumnSize(4096)));
	}

	public void testSingleRowQuery() throws Exception {
		FetchSizeCalculator calc = new FetchSizeCalculator(8 * 1024 * 1024);
		assertEquals(1, calc.getFetchSize(new Query().selectCount().from(
				new MutableTable("foo"))));
	}

	public void testMinimumSize() throws Exception {
		FetchSizeCalculator calc = new FetchSizeCalculator(8);

		assertEquals(1, calc.getFetchSize(new MutableColumn("a",
				ColumnType.INTEGER).setColumnSize(256)));
	}

	public void testMaximumSize() throws Exception {
		FetchSizeCalculator calc = new FetchSizeCalculator(Integer.MAX_VALUE);

		assertEquals(25000, calc.getFetchSize(new MutableColumn("a",
				ColumnType.BOOLEAN).setColumnSize(1)));
	}

	public void testManyColumnsScenario() throws Exception {
		Column[] cols = new Column[110];
		for (int i = 0; i < cols.length; i++) {
			MutableColumn col = new MutableColumn("foo" + i);
			if (i % 5 == 0) {
				col.setType(ColumnType.INTEGER);
			} else if (i % 4 == 0) {
				col.setType(ColumnType.BOOLEAN);
			} else if (i % 3 == 0) {
				col.setType(ColumnType.DATE);
			} else {
				col.setType(ColumnType.VARCHAR);
			}

			col.setColumnSize(50);
			cols[i] = col;
		}

		FetchSizeCalculator calc = new FetchSizeCalculator(32 * 1024 * 1024);

		assertEquals(613, calc.getFetchSize(cols));
	}

	public void testGetValueSize() throws Exception {
		FetchSizeCalculator calc = new FetchSizeCalculator(0);

		assertEquals(30, calc.getValueSize(SelectItem.getCountAllItem()));

		assertEquals(30,
				calc.getValueSize(new MutableColumn("", ColumnType.ARRAY)));
		assertEquals(300, calc.getValueSize(new MutableColumn("",
				ColumnType.ARRAY).setColumnSize(10)));

		assertEquals(16,
				calc.getValueSize(new MutableColumn("", ColumnType.INTEGER)));
		assertEquals(16,
				calc.getValueSize(new MutableColumn("", ColumnType.BIGINT)));
		assertEquals(16,
				calc.getValueSize(new MutableColumn("", ColumnType.DOUBLE)));
		assertEquals(16,
				calc.getValueSize(new MutableColumn("", ColumnType.FLOAT)));

		assertEquals(4096,
				calc.getValueSize(new MutableColumn("", ColumnType.CLOB)));
		assertEquals(4096,
				calc.getValueSize(new MutableColumn("", ColumnType.BLOB)));

		assertEquals(4096, calc.getValueSize(new MutableColumn("",
				ColumnType.CLOB).setColumnSize(20)));
		assertEquals(200000, calc.getValueSize(new MutableColumn("",
				ColumnType.BLOB).setColumnSize(200000)));
		assertEquals(400000, calc.getValueSize(new MutableColumn("",
				ColumnType.CLOB).setColumnSize(200000)));
	}

	public void testGetRowSize() throws Exception {
		FetchSizeCalculator calc = new FetchSizeCalculator(0);

		assertEquals(8000, calc.getRowSize(new MutableColumn("",
				ColumnType.VARCHAR).setColumnSize(4000)));
		assertEquals(1024,
				calc.getRowSize(new MutableColumn("", ColumnType.VARCHAR)));

		assertEquals(9024, calc.getRowSize(new MutableColumn("",
				ColumnType.VARCHAR).setColumnSize(4000), new MutableColumn("",
				ColumnType.VARCHAR)));
	}
}
