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
package org.apache.metamodel.solr;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.swing.table.TableModel;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetTableModel;
import org.apache.metamodel.data.FilteredDataSet;
import org.apache.metamodel.data.InMemoryDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class SolrDataContextTest1 {
	private static final String url = "http://localhost:8983/solr/collection1";
	private static final String index = "collection1";

	private static DataContext dataContext;

	@BeforeClass
	public static void beforeTests() throws Exception {
		DataContext dataContext = new SolrDataContext(url, index);

	}

	@AfterClass
	public static void afterTests() {
		System.out.println("Solr server shut down!");
	}

	@Test
	public void testWhereWithLimit() throws Exception {
		DataSet dataSet = dataContext
				.executeQuery("SELECT * FROM collection1 WHERE (manu='maxtor' or manu='samsung') LIMIT 1");
		try {
			assertTrue(dataSet.next());

			Row row = dataSet.getRow();
			Object val = row.getValue(1);

			assertEquals("1491367446686203904", val.toString());
		} finally {
			dataSet.close();
		}
	}

	@Test
	public void testGroupByQueryWithAlphaOrder() throws Exception {
		List<String> output = new ArrayList<String>();

		DataSet dataSet = dataContext
				.executeQuery("SELECT COUNT(*) AS X, manu FROM collection1 WHERE (manu='maxtor' OR manu='samsung') GROUP BY manu ORDER BY manu LIMIT 3");

		while (dataSet.next()) {
			Row row = dataSet.getRow();
			output.add(row.toString());
		}

		assertEquals(
				"[Row[values=[0, a]], Row[values=[0, america]], Row[values=[0, apache]]]",
				output.toString());
	}

	@Test
	public void testGroupByQueryWithMeasureOrder() throws Exception {
		List<String> output = new ArrayList<String>();

		DataSet dataSet = dataContext
				.executeQuery("SELECT COUNT(*) AS X, manu FROM collection1 WHERE (manu='maxtor' OR manu='samsung') GROUP BY manu ORDER BY X DESC LIMIT 3");

		while (dataSet.next()) {
			Row row = dataSet.getRow();
			output.add(row.toString());
		}

		assertEquals(
				"[Row[values=[1, corp]], Row[values=[1, maxtor]], Row[values=[0, a]]]",
				output.toString());
	}

	@Test
	public void testCountQuery() throws Exception {
		DataSet dataSet = dataContext
				.executeQuery("SELECT COUNT(*) FROM collection1");

		try {
			assertTrue(dataSet.next());
			assertEquals("Row[values=[32]]", dataSet.getRow().toString());
		} finally {
			dataSet.close();
		}
	}

	@Test
	public void testCountWithWhereQuery() throws Exception {
		DataSet dataSet = dataContext
				.executeQuery("SELECT COUNT(*) FROM collection1 WHERE (manu='samsung' or manu='maxtor')");

		try {
			assertTrue(dataSet.next());
			assertEquals("Row[values=[3]]", dataSet.getRow().toString());
		} finally {
			dataSet.close();
		}
	}

	@Test
	public void testGroupByQueryWithWrongMeasureOrder() throws Exception {
		boolean isThrown = false;

		try {
			DataSet dataSet = dataContext
					.executeQuery("SELECT COUNT(*) AS X, manu FROM collection1 WHERE (manu='maxtor' OR manu='samsung') GROUP BY manu ORDER BY X LIMIT 3");
			List<String> output = new ArrayList<String>();

			while (dataSet.next()) {
				Row row = dataSet.getRow();
			}
		} catch (Exception e) {
			isThrown = true;
		} finally {

		}

		assertTrue(isThrown);
	}

	@Test
	public void testGroupByQueryWithWrongAlphaOrder() throws Exception {
		boolean isThrown = false;

		try {
			DataSet dataSet = dataContext
					.executeQuery("SELECT COUNT(*) AS X, manu FROM collection1 WHERE (manu='maxtor' OR manu='samsung') GROUP BY manu ORDER BY manu DESC LIMIT 3");
			List<String> output = new ArrayList<String>();

			while (dataSet.next()) {
				Row row = dataSet.getRow();
			}
		} catch (Exception e) {
			isThrown = true;
		} finally {

		}

		assertTrue(isThrown);
	}

	@Test
	public void testQueryForANonExistingIndex() throws Exception {
		boolean isThrown = false;

		try {
			DataSet dataSet = dataContext
					.executeQuery("SELECT COUNT(*) FROM foo");
		} catch (Exception e) {
			isThrown = true;
		} finally {

		}

		assertTrue(isThrown);
	}
}