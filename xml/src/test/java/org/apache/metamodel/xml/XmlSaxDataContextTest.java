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
package org.apache.metamodel.xml;

import java.io.File;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class XmlSaxDataContextTest extends TestCase {

	public void testBasicScenarios() throws Exception {
		XmlSaxTableDef tableDef = new XmlSaxTableDef(
				"/dependencies/dependency", new String[] {
						"/dependencies/dependency/artifactId",
						"/dependencies/dependency/groupId",
						"/dependencies/dependency/version",
						"/dependencies/dependency/scope" });
		DataContext dc = new XmlSaxDataContext(new File(
				"src/test/resources/xml_input_flatten_tables.xml"), tableDef);

		final Schema schema = dc.getDefaultSchema();
		assertEquals("Schema[name=/dependencies]", schema.toString());
		assertEquals("[/dependency]", Arrays.toString(schema.getTableNames().toArray()));

		final Table table = schema.getTableByName("/dependency");
		assertEquals("[row_id, /artifactId, /groupId, /version, /scope]",
				Arrays.toString(table.getColumnNames().toArray()));

		// perform a regular query
		DataSet ds = dc.query().from(table).select(table.getColumns())
				.execute();
		assertTrue(ds.next());
		assertEquals("Row[values=[0, joda-time, joda-time, 1.5.2, compile]]",
				ds.getRow().toString());

		assertTrue(ds.next());
		assertEquals("Row[values=[1, commons-math, commons-math, 1.1, null]]",
				ds.getRow().toString());

		assertTrue(ds.next());
		assertTrue(ds.next());
		assertTrue(ds.next());
		assertTrue(ds.next());
		assertEquals("Row[values=[5, hsqldb, hsqldb, 1.8.0.7, null]]", ds
				.getRow().toString());

		assertTrue(ds.next());
		assertTrue(ds.next());
		assertTrue(ds.next());
		assertTrue(ds.next());
		assertTrue(ds.next());
		assertEquals(
				"Row[values=[10, mysql-connector-java, mysql, 5.1.6, test]]",
				ds.getRow().toString());
		assertFalse(ds.next());
		ds.close();

		// perform a constrained query
		Query q = dc.query().from(table).select("row_id").and("/groupId")
				.where("/scope").isNotNull().toQuery();
		q.setMaxRows(2);
		ds = dc.executeQuery(q);

		assertTrue(ds.next());
		assertEquals("Row[values=[0, joda-time]]", ds.getRow().toString());
		assertTrue(ds.next());
		assertEquals("Row[values=[6, junit]]", ds.getRow().toString());
		assertFalse(ds.next());
		ds.close();
	}

	public void testParentTagIndex() throws Exception {
		final XmlSaxTableDef employeeTableDef = new XmlSaxTableDef(
				"/root/organization/employees/employee", new String[] {
						"/root/organization/employees/employee/name",
						"/root/organization/employees/employee/gender",
						"index(/root/organization/employees)",
						"index(/root/organization)" });
		final XmlSaxTableDef organizationTableDef = new XmlSaxTableDef(
				"/root/organization", new String[] { "/root/organization/name",
						"/root/organization@type" });

		final DataContext dc = new XmlSaxDataContext(
				new File(
						"src/test/resources/xml_input_parent_and_child_relationship.xml"),
				employeeTableDef, organizationTableDef);

		final Schema schema = dc.getDefaultSchema();
		assertEquals("Schema[name=/root]", schema.toString());
		assertEquals("[/employee, /organization]",
				Arrays.toString(schema.getTableNames().toArray()));

		// organization assertions
		final Table organizationTable = schema.getTableByName("/organization");
		{
			assertEquals("[row_id, /name, @type]",
					Arrays.toString(organizationTable.getColumnNames().toArray()));

			DataSet ds = dc.query().from(organizationTable)
					.select(organizationTable.getColumns()).execute();
			assertTrue(ds.next());
			assertEquals("Row[values=[0, Company A, governmental]]", ds
					.getRow().toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[1, Company B, company]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[2, Company C, company]]", ds.getRow()
					.toString());
			assertFalse(ds.next());
			ds.close();
		}

		// employee assertions
		final Table employeeTable = schema.getTableByName("/employee");
		{
			assertEquals(
					"[row_id, /name, /gender, index(/root/organization/employees), index(/root/organization)]",
					Arrays.toString(employeeTable.getColumnNames().toArray()));

			DataSet ds = dc.query().from(employeeTable)
					.select(employeeTable.getColumns()).execute();
			assertTrue(ds.next());
			assertEquals("Row[values=[0, John Doe, M, 0, 0]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[1, Jane Doe, F, 0, 0]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[2, Peter, M, 1, 1]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[3, Bob, M, 1, 1]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[4, Cindy, F, 1, 1]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[5, John, M, 1, 1]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[6, James, M, 2, 2]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[7, Suzy, F, 2, 2]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[8, Carla, F, 3, 2]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[9, Vincent, M, 3, 2]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[10, Barbara, F, 3, 2]]", ds.getRow()
					.toString());
			assertFalse(ds.next());
			ds.close();
		}

		// do a join
		{
			Column fk = employeeTable
					.getColumnByName("index(/root/organization)");
			assertNotNull(fk);
			Column empName = employeeTable.getColumnByName("/name");
			assertNotNull(empName);
			Column orgId = organizationTable.getColumnByName("row_id");
			assertNotNull(orgId);
			Column orgName = organizationTable.getColumnByName("/name");
			assertNotNull(orgName);

			Query q = dc.query().from(employeeTable)
					.innerJoin(organizationTable).on(fk, orgId).select(empName)
					.as("employee").select(orgName).as("company").toQuery();

			assertEquals(
					"SELECT /employee./name AS employee, /organization./name AS company "
							+ "FROM /root./employee INNER JOIN /root./organization "
							+ "ON /employee.index(/root/organization) = /organization.row_id",
					q.toSql());

			DataSet ds = dc.executeQuery(q);
			assertTrue(ds.next());
			assertEquals("Row[values=[John Doe, Company A]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[Jane Doe, Company A]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[Peter, Company B]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[Bob, Company B]]", ds.getRow().toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[Cindy, Company B]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[John, Company B]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[James, Company C]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[Suzy, Company C]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[Carla, Company C]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[Vincent, Company C]]", ds.getRow()
					.toString());
			assertTrue(ds.next());
			assertEquals("Row[values=[Barbara, Company C]]", ds.getRow()
					.toString());
			ds.close();
		}
	}
}
