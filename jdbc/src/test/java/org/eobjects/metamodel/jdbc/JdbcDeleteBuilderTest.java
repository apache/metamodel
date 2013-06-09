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

public class JdbcDeleteBuilderTest extends JdbcTestCase {

    public void testCreateSqlStatement() throws Exception {
        JdbcDataContext dataContext = new JdbcDataContext(getTestDbConnection());
        Table table = dataContext.getTableByQualifiedLabel("PUBLIC.EMPLOYEES");
        JdbcUpdateCallback updateCallback = new JdbcSimpleUpdateCallback(dataContext);
        JdbcDeleteBuilder updateBuilder = new JdbcDeleteBuilder(updateCallback, table, new DefaultQueryRewriter(
                dataContext), true);

        updateBuilder.where("REPORTSTO").eq(1234);
        assertEquals("DELETE FROM PUBLIC._EMPLOYEES_ WHERE _EMPLOYEES_._REPORTSTO_ = 1234",
                updateBuilder.createSqlStatement().replaceAll("\"", "_"));

        updateBuilder.where("JOBTITLE").eq("Sales rep");
        assertEquals(
                "DELETE FROM PUBLIC._EMPLOYEES_ WHERE _EMPLOYEES_._REPORTSTO_ = 1234 AND _EMPLOYEES_._JOBTITLE_ = 'Sales rep'",
                updateBuilder.createSqlStatement().replaceAll("\"", "_"));
    }

    public void testInsertNulls() throws Exception {
        JdbcDataContext dataContext = new JdbcDataContext(getTestDbConnection());
        Table table = dataContext.getTableByQualifiedLabel("PUBLIC.EMPLOYEES");
        JdbcUpdateCallback updateCallback = new JdbcSimpleUpdateCallback(dataContext);
        JdbcDeleteBuilder updateBuilder = new JdbcDeleteBuilder(updateCallback, table, new DefaultQueryRewriter(
                dataContext), true);

        assertEquals("[EMPLOYEENUMBER, LASTNAME, FIRSTNAME, EXTENSION, EMAIL, OFFICECODE, REPORTSTO, JOBTITLE]",
                Arrays.toString(table.getColumnNames()));

        updateBuilder.where("firstname").isNull().where("lastname").isNotNull();
        assertEquals("DELETE FROM PUBLIC._EMPLOYEES_ WHERE _EMPLOYEES_._FIRSTNAME_ IS NULL AND _EMPLOYEES_._LASTNAME_ IS NOT NULL", updateBuilder
                .createSqlStatement().replaceAll("\"", "_"));
    }

    public void testCreateSqlStatementWithQuotesInValue() throws Exception {
        JdbcDataContext dataContext = new JdbcDataContext(getTestDbConnection());
        Table table = dataContext.getTableByQualifiedLabel("PUBLIC.EMPLOYEES");
        JdbcUpdateCallback updateCallback = new JdbcSimpleUpdateCallback(dataContext);
        JdbcDeleteBuilder updateBuilder = new JdbcDeleteBuilder(updateCallback, table, new DefaultQueryRewriter(
                dataContext), true);
        
        updateBuilder.where("OFFICECODE").isEquals("ro'om");
        assertEquals(
                "DELETE FROM PUBLIC._EMPLOYEES_ WHERE _EMPLOYEES_._OFFICECODE_ = 'ro''om'",
                updateBuilder.createSqlStatement().replaceAll("\"", "_"));
    }
}
