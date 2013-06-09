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

public class JdbcUpdateBuilderTest extends JdbcTestCase {

    public void testCreateSqlStatement() throws Exception {
        JdbcDataContext dataContext = new JdbcDataContext(getTestDbConnection());
        Table table = dataContext.getTableByQualifiedLabel("PUBLIC.EMPLOYEES");
        JdbcUpdateCallback updateCallback = new JdbcSimpleUpdateCallback(dataContext);
        JdbcUpdateBuilder updateBuilder = new JdbcUpdateBuilder(updateCallback, table, new DefaultQueryRewriter(
                dataContext), true);

        assertEquals("[EMPLOYEENUMBER, LASTNAME, FIRSTNAME, EXTENSION, EMAIL, OFFICECODE, REPORTSTO, JOBTITLE]",
                Arrays.toString(table.getColumnNames()));

        updateBuilder.value("LASTNAME", "foo").value("firstname", "BAR");
        assertEquals("UPDATE PUBLIC._EMPLOYEES_ SET LASTNAME='foo',FIRSTNAME='BAR'", updateBuilder.createSqlStatement()
                .replaceAll("\"", "_"));

        updateBuilder.where("REPORTSTO").isEquals(1234);
        assertEquals(
                "UPDATE PUBLIC._EMPLOYEES_ SET LASTNAME='foo',FIRSTNAME='BAR' WHERE _EMPLOYEES_._REPORTSTO_ = 1234",
                updateBuilder.createSqlStatement().replaceAll("\"", "_"));

        updateBuilder.where("JOBTITLE").isEquals("Sales rep");
        assertEquals(
                "UPDATE PUBLIC._EMPLOYEES_ SET LASTNAME='foo',FIRSTNAME='BAR' WHERE _EMPLOYEES_._REPORTSTO_ = 1234 AND _EMPLOYEES_._JOBTITLE_ = 'Sales rep'",
                updateBuilder.createSqlStatement().replaceAll("\"", "_"));
    }

    public void testInsertNulls() throws Exception {
        JdbcDataContext dataContext = new JdbcDataContext(getTestDbConnection());
        Table table = dataContext.getTableByQualifiedLabel("PUBLIC.EMPLOYEES");
        JdbcUpdateCallback updateCallback = new JdbcSimpleUpdateCallback(dataContext);
        JdbcUpdateBuilder updateBuilder = new JdbcUpdateBuilder(updateCallback, table, new DefaultQueryRewriter(
                dataContext), true);

        assertEquals("[EMPLOYEENUMBER, LASTNAME, FIRSTNAME, EXTENSION, EMAIL, OFFICECODE, REPORTSTO, JOBTITLE]",
                Arrays.toString(table.getColumnNames()));

        updateBuilder.value("LASTNAME", "foo").value("firstname", null);
        assertEquals("UPDATE PUBLIC._EMPLOYEES_ SET LASTNAME='foo',FIRSTNAME=NULL", updateBuilder.createSqlStatement()
                .replaceAll("\"", "_"));
    }

    public void testCreateSqlStatementWithQuotesInValue() throws Exception {
        JdbcDataContext dataContext = new JdbcDataContext(getTestDbConnection());
        Table table = dataContext.getTableByQualifiedLabel("PUBLIC.EMPLOYEES");
        JdbcUpdateCallback updateCallback = new JdbcSimpleUpdateCallback(dataContext);
        JdbcUpdateBuilder updateBuilder = new JdbcUpdateBuilder(updateCallback, table, new DefaultQueryRewriter(
                dataContext), true);

        assertEquals("[EMPLOYEENUMBER, LASTNAME, FIRSTNAME, EXTENSION, EMAIL, OFFICECODE, REPORTSTO, JOBTITLE]",
                Arrays.toString(table.getColumnNames()));

        updateBuilder.value("LASTNAME", "foo").value("firstname", "BAR");
        assertEquals("UPDATE PUBLIC._EMPLOYEES_ SET LASTNAME='foo',FIRSTNAME='BAR'", updateBuilder.createSqlStatement()
                .replaceAll("\"", "_"));

        updateBuilder.value(4, "foo@'bar.com");
        updateBuilder.value("REPORTSTO", 1234);
        updateBuilder.where("OFFICECODE").isEquals("ro'om");
        assertEquals(
                "UPDATE PUBLIC._EMPLOYEES_ SET LASTNAME='foo',FIRSTNAME='BAR',EMAIL='foo@''bar.com',REPORTSTO=1234 WHERE _EMPLOYEES_._OFFICECODE_ = 'ro''om'",
                updateBuilder.createSqlStatement().replaceAll("\"", "_"));
    }
}
