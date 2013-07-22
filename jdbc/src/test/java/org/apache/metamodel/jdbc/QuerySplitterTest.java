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
import java.util.Arrays;
import java.util.List;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.JoinType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class QuerySplitterTest extends JdbcTestCase {

	public void testSimpleQuerySplit() throws Exception {
		Connection con = getTestDbConnection();
		DataContext dc = new JdbcDataContext(con);
		Schema schema = dc.getSchemaByName("PUBLIC");
		Table employeesTable = schema.getTableByName("EMPLOYEES");
		Table customersTable = schema.getTableByName("CUSTOMERS");
		Query q = new Query().from(employeesTable, "e").from(customersTable,
				"c");
		q.select(employeesTable.getColumns()[0], customersTable.getColumns()[0]);
		assertEquals(
				"SELECT e._EMPLOYEENUMBER_, c._CUSTOMERNUMBER_ FROM PUBLIC._EMPLOYEES_ e, PUBLIC._CUSTOMERS_ c",
				q.toString().replace('\"', '_'));

		QuerySplitter qs = new QuerySplitter(dc, q);
		long rowCount = qs.getRowCount();
		assertEquals(2806, rowCount);

		qs.setMaxRows(1000);
		List<Query> splitQueries = qs.splitQuery();

		assertEquals("[793, 610, 366, 714, 323]",
				Arrays.toString(getCounts(dc, splitQueries)));

		assertEquals(
				"[SELECT e._EMPLOYEENUMBER_, c._CUSTOMERNUMBER_ FROM PUBLIC._EMPLOYEES_ e, PUBLIC._CUSTOMERS_ c WHERE (c._CUSTOMERNUMBER_ < 299 OR c._CUSTOMERNUMBER_ IS NULL) AND (e._EMPLOYEENUMBER_ < 1352 OR e._EMPLOYEENUMBER_ IS NULL), SELECT e._EMPLOYEENUMBER_, c._CUSTOMERNUMBER_ FROM PUBLIC._EMPLOYEES_ e, PUBLIC._CUSTOMERS_ c WHERE (c._CUSTOMERNUMBER_ < 299 OR c._CUSTOMERNUMBER_ IS NULL) AND (e._EMPLOYEENUMBER_ > 1352 OR e._EMPLOYEENUMBER_ = 1352), SELECT e._EMPLOYEENUMBER_, c._CUSTOMERNUMBER_ FROM PUBLIC._EMPLOYEES_ e, PUBLIC._CUSTOMERS_ c WHERE (c._CUSTOMERNUMBER_ > 299 OR c._CUSTOMERNUMBER_ = 299) AND (e._REPORTSTO_ < 1072 OR e._REPORTSTO_ IS NULL), SELECT e._EMPLOYEENUMBER_, c._CUSTOMERNUMBER_ FROM PUBLIC._EMPLOYEES_ e, PUBLIC._CUSTOMERS_ c WHERE (c._CUSTOMERNUMBER_ > 299 OR c._CUSTOMERNUMBER_ = 299) AND (e._REPORTSTO_ > 1072 OR e._REPORTSTO_ = 1072) AND (c._SALESREPEMPLOYEENUMBER_ < 1433 OR c._SALESREPEMPLOYEENUMBER_ IS NULL), SELECT e._EMPLOYEENUMBER_, c._CUSTOMERNUMBER_ FROM PUBLIC._EMPLOYEES_ e, PUBLIC._CUSTOMERS_ c WHERE (c._CUSTOMERNUMBER_ > 299 OR c._CUSTOMERNUMBER_ = 299) AND (e._REPORTSTO_ > 1072 OR e._REPORTSTO_ = 1072) AND (c._SALESREPEMPLOYEENUMBER_ > 1433 OR c._SALESREPEMPLOYEENUMBER_ = 1433)]",
				Arrays.toString(splitQueries.toArray()).replace('\"', '_'));
		assertSameCount(dc, qs, splitQueries);
		assertEquals(5, splitQueries.size());

		splitQueries = qs.setMaxRows(300).splitQuery();
		assertSameCount(dc, qs, splitQueries);
		assertEquals(
				"[299, 221, 170, 299, 276, 253, 102, 289, 253, 138, 368, 138]",
				Arrays.toString(getCounts(dc, splitQueries)));

		splitQueries = qs.setMaxRows(5000).splitQuery();
		assertEquals(1, splitQueries.size());
		assertSame(q, splitQueries.get(0));
	}

	public void testGroupByQuery() throws Exception {
		Connection con = getTestDbConnection();
		DataContext dc = new JdbcDataContext(con);
		Schema schema = dc.getDefaultSchema();
		Table employeesTable = schema.getTableByName("EMPLOYEES");
		Table orderDetailsTable = schema.getTableByName("ORDERDETAILS");
		Query q = new Query().from(employeesTable, "e").from(orderDetailsTable,
				"c");
		q.select(orderDetailsTable.getColumns()[0])
				.select(new SelectItem(FunctionType.MAX, employeesTable
						.getColumns()[0]));
		q.groupBy(orderDetailsTable.getColumns()[0]);
		assertEquals(
				"SELECT c._ORDERNUMBER_, MAX(e._EMPLOYEENUMBER_) FROM PUBLIC._EMPLOYEES_ e, PUBLIC._ORDERDETAILS_ c GROUP BY c._ORDERNUMBER_",
				q.toString().replace('\"', '_'));

		QuerySplitter qs = new QuerySplitter(dc, q);
		assertEquals(326, qs.getRowCount());

		List<Query> splitQueries = qs.setMaxRows(250).splitQuery();

		assertEquals(
				"[SELECT c._ORDERNUMBER_, MAX(e._EMPLOYEENUMBER_) FROM PUBLIC._EMPLOYEES_ e, PUBLIC._ORDERDETAILS_ c WHERE (c._ORDERNUMBER_ < 10262 OR c._ORDERNUMBER_ IS NULL) GROUP BY c._ORDERNUMBER_, SELECT c._ORDERNUMBER_, MAX(e._EMPLOYEENUMBER_) FROM PUBLIC._EMPLOYEES_ e, PUBLIC._ORDERDETAILS_ c WHERE (c._ORDERNUMBER_ > 10262 OR c._ORDERNUMBER_ = 10262) GROUP BY c._ORDERNUMBER_]",
				Arrays.toString(splitQueries.toArray()).replace('\"', '_'));
		assertSameCount(dc, qs, splitQueries);
		assertEquals(2, splitQueries.size());
		assertEquals("[162, 164]", Arrays.toString(getCounts(dc, splitQueries)));

		DataSet data = qs.executeQueries();
		int count = 0;
		while (data.next()) {
			if (count == 2) {
				assertEquals("Row[values=[10102, 1702]]", data.getRow()
						.toString());
			}
			count++;
		}
		data.close();
		assertEquals(326, count);
	}

	public void testSplitJoinQuery() throws Exception {
		Connection con = getTestDbConnection();
		DataContext dc = new JdbcDataContext(con);
		Schema schema = dc.getDefaultSchema();
		Table productsTable = schema.getTableByName("PRODUCTS");
		Relationship[] relationships = productsTable.getRelationships();
		Relationship relationship = relationships[0];
		assertEquals(
				"Relationship[primaryTable=PRODUCTS,primaryColumns=[PRODUCTCODE],foreignTable=ORDERFACT,foreignColumns=[PRODUCTCODE]]",
				relationship.toString());

		Query q = new Query().from(new FromItem(JoinType.LEFT, relationship))
				.select(relationship.getForeignColumns())
				.select(relationship.getPrimaryColumns());
		assertEquals(
				"SELECT _ORDERFACT_._PRODUCTCODE_, _PRODUCTS_._PRODUCTCODE_ FROM PUBLIC._PRODUCTS_ LEFT JOIN PUBLIC._ORDERFACT_ ON _PRODUCTS_._PRODUCTCODE_ = _ORDERFACT_._PRODUCTCODE_",
				q.toString().replace('\"', '_'));

		QuerySplitter qs = new QuerySplitter(dc, q);
		assertEquals(2997, qs.getRowCount());

		List<Query> splitQueries = qs.setMaxRows(1500).splitQuery();
		assertSameCount(dc, qs, splitQueries);
		assertEquals(3, splitQueries.size());
		assertEquals("[1415, 902, 680]",
				Arrays.toString(getCounts(dc, splitQueries)));
	}

	public void testSplitSubQuery() throws Exception {
		Connection con = getTestDbConnection();
		DataContext dc = new JdbcDataContext(con);
		Schema schema = dc.getDefaultSchema();
		Table employeesTable = schema.getTableByName("EMPLOYEES");
		Table customersTable = schema.getTableByName("CUSTOMERS");
		Query sq = new Query().from(employeesTable, "e").from(customersTable,
				"c");
		SelectItem empSelectItem = new SelectItem(
				employeesTable.getColumns()[0]);
		SelectItem custSelectItem = new SelectItem(
				customersTable.getColumns()[0]);
		sq.select(empSelectItem, custSelectItem);
		assertEquals(
				"SELECT e._EMPLOYEENUMBER_, c._CUSTOMERNUMBER_ FROM PUBLIC._EMPLOYEES_ e, PUBLIC._CUSTOMERS_ c",
				sq.toString().replace('\"', '_'));
		Query q = new Query();
		FromItem sqItem = new FromItem(sq).setAlias("sq");

		custSelectItem.setAlias("c_num");
		empSelectItem.setAlias("e_num");

		q.from(sqItem);
		q.select(new SelectItem(custSelectItem, sqItem), new SelectItem(
				empSelectItem, sqItem));
		assertEquals(
				"SELECT sq.c_num, sq.e_num FROM (SELECT e._EMPLOYEENUMBER_ AS e_num, c._CUSTOMERNUMBER_ AS c_num FROM PUBLIC._EMPLOYEES_ e, PUBLIC._CUSTOMERS_ c) sq",
				q.toString().replace('\"', '_'));

		QuerySplitter qs = new QuerySplitter(dc, q);
		assertEquals(2806, qs.getRowCount());

		List<Query> splitQueries = qs.setMaxRows(1000).splitQuery();
		assertSameCount(dc, qs, splitQueries);
		assertEquals(5, splitQueries.size());
		assertEquals("[793, 610, 366, 714, 323]",
				Arrays.toString(getCounts(dc, splitQueries)));

		splitQueries = qs.setMaxRows(2000).splitQuery();
		assertSameCount(dc, qs, splitQueries);
		assertEquals(2, splitQueries.size());
		assertEquals("[1403, 1403]",
				Arrays.toString(getCounts(dc, splitQueries)));

		DataSet data = qs.executeQueries();
		int count = 0;
		while (data.next()) {
			if (count == 2) {
				assertEquals("Row[values=[114, 1002]]", data.getRow()
						.toString());
			}
			count++;
		}
		data.close();
		assertEquals(2806, count);
	}

	/**
	 * Utility method for asserting that a query and it's splitted queries have
	 * the same total count
	 */
	private void assertSameCount(DataContext dc, QuerySplitter qs,
			List<Query> queries) {
		long count1 = qs.getRowCount();
		long count2 = 0;
		for (Query q : queries) {
			count2 += getCount(dc, q);
		}
		assertEquals(count1, count2);
	}

	public long[] getCounts(DataContext dc, List<Query> queries) {
		long[] result = new long[queries.size()];
		for (int i = 0; i < result.length; i++) {
			result[i] = getCount(dc, queries.get(i));
		}
		return result;
	}

	/**
	 * Gets the count of a query
	 */
	private long getCount(DataContext dc, Query query) {
		return new QuerySplitter(dc, query).getRowCount();
	}
}