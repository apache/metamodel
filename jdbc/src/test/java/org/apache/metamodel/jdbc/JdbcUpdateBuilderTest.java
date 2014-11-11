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

    public void testUpdateWhereSomethingIsOrIsNotNull() throws Exception {
        JdbcDataContext dataContext = new JdbcDataContext(getTestDbConnection());
        Table table = dataContext.getTableByQualifiedLabel("PUBLIC.EMPLOYEES");
        JdbcUpdateCallback updateCallback = new JdbcSimpleUpdateCallback(dataContext);

        assertEquals("[EMPLOYEENUMBER, LASTNAME, FIRSTNAME, EXTENSION, EMAIL, OFFICECODE, REPORTSTO, JOBTITLE]",
                Arrays.toString(table.getColumnNames()));

        JdbcUpdateBuilder updateBuilder = new JdbcUpdateBuilder(updateCallback, table, new DefaultQueryRewriter(
                dataContext), false);
        updateBuilder.value("LASTNAME", "foo").where("email").isNull().where("officecode").isNotNull();
        assertEquals(
                "UPDATE PUBLIC._EMPLOYEES_ SET LASTNAME=? WHERE _EMPLOYEES_._EMAIL_ IS NULL AND _EMPLOYEES_._OFFICECODE_ IS NOT NULL",
                updateBuilder.createSqlStatement().replaceAll("\"", "_"));
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
