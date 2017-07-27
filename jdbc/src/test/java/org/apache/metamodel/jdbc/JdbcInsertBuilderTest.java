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

import java.util.Arrays;

import org.apache.metamodel.jdbc.dialects.DefaultQueryRewriter;
import org.apache.metamodel.schema.Table;

public class JdbcInsertBuilderTest extends JdbcTestCase {

	public void testCreateSqlStatement() throws Exception {
		JdbcDataContext dataContext = new JdbcDataContext(getTestDbConnection());
		Table table = dataContext.getTableByQualifiedLabel("PUBLIC.EMPLOYEES");
		JdbcUpdateCallback updateCallback = new JdbcSimpleUpdateCallback(dataContext);
		JdbcInsertBuilder insertBuilder = new JdbcInsertBuilder(updateCallback, table, true,
				new DefaultQueryRewriter(dataContext));

		assertEquals("[EMPLOYEENUMBER, LASTNAME, FIRSTNAME, EXTENSION, EMAIL, OFFICECODE, REPORTSTO, JOBTITLE]",
				Arrays.toString(table.getColumnNames().toArray()));

		insertBuilder.value("LASTNAME", "foo").value("firstname", "BAR");
		assertEquals("INSERT INTO PUBLIC._EMPLOYEES_ (LASTNAME,FIRSTNAME) VALUES ('foo','BAR')", insertBuilder.createSqlStatement()
				.replaceAll("\"", "_"));

		insertBuilder.value(4, "foo@bar.com");
		insertBuilder.value("REPORTSTO", 1234);
		assertEquals("INSERT INTO PUBLIC._EMPLOYEES_ (LASTNAME,FIRSTNAME,EMAIL,REPORTSTO) VALUES ('foo','BAR','foo@bar.com',1234)",
				insertBuilder.createSqlStatement().replaceAll("\"", "_"));
	}

	public void testInsertNulls() throws Exception {
		JdbcDataContext dataContext = new JdbcDataContext(getTestDbConnection());
		Table table = dataContext.getTableByQualifiedLabel("PUBLIC.EMPLOYEES");
		JdbcUpdateCallback updateCallback = new JdbcSimpleUpdateCallback(dataContext);
		JdbcInsertBuilder insertBuilder = new JdbcInsertBuilder(updateCallback, table, true,
				new DefaultQueryRewriter(dataContext));

		assertEquals("[EMPLOYEENUMBER, LASTNAME, FIRSTNAME, EXTENSION, EMAIL, OFFICECODE, REPORTSTO, JOBTITLE]",
				Arrays.toString(table.getColumnNames().toArray()));

		insertBuilder.value("LASTNAME", "foo").value("firstname", null);
		assertEquals("INSERT INTO PUBLIC._EMPLOYEES_ (LASTNAME,FIRSTNAME) VALUES ('foo',NULL)", insertBuilder.createSqlStatement()
				.replaceAll("\"", "_"));
	}

	public void testCreateSqlStatementWithQuotesInValue() throws Exception {
		JdbcDataContext dataContext = new JdbcDataContext(getTestDbConnection());
		Table table = dataContext.getTableByQualifiedLabel("PUBLIC.EMPLOYEES");
		JdbcUpdateCallback updateCallback = new JdbcSimpleUpdateCallback(dataContext);
		JdbcInsertBuilder insertBuilder = new JdbcInsertBuilder(updateCallback, table, true,
				new DefaultQueryRewriter(dataContext));

		assertEquals("[EMPLOYEENUMBER, LASTNAME, FIRSTNAME, EXTENSION, EMAIL, OFFICECODE, REPORTSTO, JOBTITLE]",
				Arrays.toString(table.getColumnNames().toArray()));

		insertBuilder.value("LASTNAME", "foo").value("firstname", "BAR");
		assertEquals("INSERT INTO PUBLIC._EMPLOYEES_ (LASTNAME,FIRSTNAME) VALUES ('foo','BAR')", insertBuilder.createSqlStatement()
				.replaceAll("\"", "_"));

		insertBuilder.value(4, "foo@'bar.com");
		insertBuilder.value("REPORTSTO", 1234);
		assertEquals(
				"INSERT INTO PUBLIC._EMPLOYEES_ (LASTNAME,FIRSTNAME,EMAIL,REPORTSTO) VALUES ('foo','BAR','foo@''bar.com',1234)",
				insertBuilder.createSqlStatement().replaceAll("\"", "_"));
	}
}
