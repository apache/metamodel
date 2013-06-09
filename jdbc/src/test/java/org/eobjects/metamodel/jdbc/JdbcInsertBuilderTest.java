/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.jdbc;

import java.util.Arrays;

import org.eobjects.metamodel.jdbc.dialects.DefaultQueryRewriter;
import org.eobjects.metamodel.schema.Table;

public class JdbcInsertBuilderTest extends JdbcTestCase {

	public void testCreateSqlStatement() throws Exception {
		JdbcDataContext dataContext = new JdbcDataContext(getTestDbConnection());
		Table table = dataContext.getTableByQualifiedLabel("PUBLIC.EMPLOYEES");
		JdbcUpdateCallback updateCallback = new JdbcSimpleUpdateCallback(dataContext);
		JdbcInsertBuilder insertBuilder = new JdbcInsertBuilder(updateCallback, table, true,
				new DefaultQueryRewriter(dataContext));

		assertEquals("[EMPLOYEENUMBER, LASTNAME, FIRSTNAME, EXTENSION, EMAIL, OFFICECODE, REPORTSTO, JOBTITLE]",
				Arrays.toString(table.getColumnNames()));

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
				Arrays.toString(table.getColumnNames()));

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
				Arrays.toString(table.getColumnNames()));

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
