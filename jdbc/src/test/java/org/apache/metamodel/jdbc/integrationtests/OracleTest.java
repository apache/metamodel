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
package org.apache.metamodel.jdbc.integrationtests;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import javax.swing.table.TableModel;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetTableModel;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.jdbc.JdbcTestTemplates;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.jdbc.dialects.OracleQueryRewriter;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.JoinType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.TableType;

/**
 * Test case that tests oracle interaction. An express edition of the oracle
 * database can be used to run these tests.
 * 
 * The test requires the "human resources" schema that is provided ass a sample
 * schema for Oracle default installations.
 * 
 * The script for installing it can be found in:
 * 
 * <pre>
 * $ORACLE_HOME / demo / schema / human_resources / hr_main.sql
 * </pre>
 * 
 * Install with something like:
 * 
 * <pre>
 * $ORACLE_HOME/bin/sqlplus -S &quot;/ as sysdba&quot; @hr_main.sql
 * </pre>
 * 
 * The JDBC driver is not available in the Maven repository so you will have to
 * download and attach it to the eclipse project yourself.
 * 
 * @see http://www.oracle.com/technology/products/bi/samples
 * @see http
 *      ://www.oracle.com/technology/software/products/database/xe/index.html
 */
public class OracleTest extends AbstractJdbIntegrationTest {

    @Override
    protected String getPropertyPrefix() {
        return "oracle";
    }
    
    public void testGetQueryRewriter() throws Exception {
        if (!isConfigured()) {
            return;
        }

        IQueryRewriter queryRewriter = getDataContext().getQueryRewriter();
        assertEquals(OracleQueryRewriter.class, queryRewriter.getClass());
    }

    public void testCreateInsertAndUpdate() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcTestTemplates.simpleCreateInsertUpdateAndDrop(getDataContext(), "metamodel_test_simple");
    }
    
    public void testTimestampValueInsertSelect() throws Exception {
        if (!isConfigured()) {
            return;
        }
        
        final Connection connection = getConnection();
        JdbcTestTemplates.timestampValueInsertSelect(connection, TimeUnit.MICROSECONDS, null);
    }

    public void testCompositePrimaryKeyCreation() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcTestTemplates.compositeKeyCreation(getDataContext(), "metamodel_test_composite_keys");
    }

    /**
     * Ticket #170: getIndexInfo causes SQLException. We test that resultsets
     * are closed properly.
     */
    public void testIndexInfo() throws Exception {
        if (!isConfigured()) {
            return;
        }

        Schema schema = new JdbcDataContext(getConnection(), new TableType[] { TableType.TABLE }, null)
                .getSchemaByName("SYS");
        assertEquals(12, schema.getTableCount());
    }

    public void testGetSchemaNames() throws Exception {
        if (!isConfigured()) {
            return;
        }
        DataContext dc = new JdbcDataContext(getConnection());
        String[] schemaNames = dc.getSchemaNames();

        String concatSchemas = Arrays.toString(schemaNames);

        // In order to allow the database to be used for other purposes than
        // this integration test, we will not make an exact assertion as to
        // which schema names exist, but just assert that HR and the default
        // oracle schemas exist.
        assertTrue(concatSchemas.indexOf("foobar_schema_that_does_not_exist") == -1);
        assertTrue(concatSchemas.indexOf("HR") != -1);
        assertTrue(concatSchemas.indexOf("SYSTEM") != -1);
        assertTrue(concatSchemas.indexOf("XDB") != -1);
        assertTrue(schemaNames.length > 8);

        Schema schema = dc.getDefaultSchema();
        assertEquals("HR", schema.getName());
    }

    /**
     * Really only tests the JDBC implementation, used to help localize the
     * cause for Ticket #144
     */
    public void testGetImportedKeys() throws Exception {
        if (!isConfigured()) {
            return;
        }
        ResultSet rs = getConnection().getMetaData().getImportedKeys(null, "HR", "EMPLOYEES");
        int count = 0;
        while (rs.next()) {
            count++;
            assertEquals("HR", rs.getString(2));
            String pkTableName = rs.getString(3);
            String pkColumnName = rs.getString(4);
            String fkTableName = rs.getString(7);
            assertEquals("EMPLOYEES", fkTableName);
            String fkColumnName = rs.getString(8);
            System.out.println("Found primary key relation: pkTableName=" + pkTableName + ",pkColumnName="
                    + pkColumnName + ",fkTableName=" + fkTableName + ",fkColumnName=" + fkColumnName);
        }
        rs.close();
        assertEquals(3, count);

        rs = getConnection().getMetaData().getImportedKeys(null, "HR", "DEPARTMENTS");
        count = 0;
        while (rs.next()) {
            count++;
            assertEquals("HR", rs.getString(2));
            String pkTableName = rs.getString(3);
            String pkColumnName = rs.getString(4);
            String fkTableName = rs.getString(7);
            assertEquals("DEPARTMENTS", fkTableName);
            String fkColumnName = rs.getString(8);
            System.out.println("Found primary key relation: pkTableName=" + pkTableName + ",pkColumnName="
                    + pkColumnName + ",fkTableName=" + fkTableName + ",fkColumnName=" + fkColumnName);
        }
        rs.close();
        assertEquals(2, count);
    }

    public void testGetSchema() throws Exception {
        if (!isConfigured()) {
            return;
        }
        Schema schema = getDataContext().getSchemaByName("HR");
        assertNotNull(schema);
        assertEquals("{JdbcTable[name=COUNTRIES,type=TABLE,remarks=<null>],"
                + "JdbcTable[name=DEPARTMENTS,type=TABLE,remarks=<null>]"
                + ",JdbcTable[name=EMPLOYEES,type=TABLE,remarks=<null>]"
                + ",JdbcTable[name=JOBS,type=TABLE,remarks=<null>]"
                + ",JdbcTable[name=JOB_HISTORY,type=TABLE,remarks=<null>]"
                + ",JdbcTable[name=LOCATIONS,type=TABLE,remarks=<null>]"
                + ",JdbcTable[name=REGIONS,type=TABLE,remarks=<null>]"
                + ",JdbcTable[name=EMP_DETAILS_VIEW,type=VIEW,remarks=<null>]}", Arrays.toString(schema.getTables()));

        Relationship[] employeeRelationships = schema.getTableByName("EMPLOYEES").getRelationships();
        assertEquals(
                "{Relationship[primaryTable=EMPLOYEES,primaryColumns={EMPLOYEE_ID},foreignTable=DEPARTMENTS,foreignColumns={MANAGER_ID}],"
                        + "Relationship[primaryTable=DEPARTMENTS,primaryColumns={DEPARTMENT_ID},foreignTable=EMPLOYEES,foreignColumns={DEPARTMENT_ID}],"
                        + "Relationship[primaryTable=EMPLOYEES,primaryColumns={EMPLOYEE_ID},foreignTable=EMPLOYEES,foreignColumns={MANAGER_ID}],"
                        + "Relationship[primaryTable=JOBS,primaryColumns={JOB_ID},foreignTable=EMPLOYEES,foreignColumns={JOB_ID}],"
                        + "Relationship[primaryTable=EMPLOYEES,primaryColumns={EMPLOYEE_ID},foreignTable=JOB_HISTORY,foreignColumns={EMPLOYEE_ID}]}",
                Arrays.toString(employeeRelationships));

        assertEquals(
                "{JdbcColumn[name=EMPLOYEE_ID,columnNumber=0,type=DECIMAL,nullable=false,nativeType=NUMBER,columnSize=6],"
                        + "JdbcColumn[name=FIRST_NAME,columnNumber=1,type=VARCHAR,nullable=true,nativeType=VARCHAR2,columnSize=20],"
                        + "JdbcColumn[name=LAST_NAME,columnNumber=2,type=VARCHAR,nullable=false,nativeType=VARCHAR2,columnSize=25],"
                        + "JdbcColumn[name=EMAIL,columnNumber=3,type=VARCHAR,nullable=false,nativeType=VARCHAR2,columnSize=25],"
                        + "JdbcColumn[name=PHONE_NUMBER,columnNumber=4,type=VARCHAR,nullable=true,nativeType=VARCHAR2,columnSize=20],"
                        + "JdbcColumn[name=HIRE_DATE,columnNumber=5,type=DATE,nullable=false,nativeType=DATE,columnSize=7],"
                        + "JdbcColumn[name=JOB_ID,columnNumber=6,type=VARCHAR,nullable=false,nativeType=VARCHAR2,columnSize=10],"
                        + "JdbcColumn[name=SALARY,columnNumber=7,type=DECIMAL,nullable=true,nativeType=NUMBER,columnSize=8],"
                        + "JdbcColumn[name=COMMISSION_PCT,columnNumber=8,type=DECIMAL,nullable=true,nativeType=NUMBER,columnSize=2],"
                        + "JdbcColumn[name=MANAGER_ID,columnNumber=9,type=DECIMAL,nullable=true,nativeType=NUMBER,columnSize=6],"
                        + "JdbcColumn[name=DEPARTMENT_ID,columnNumber=10,type=DECIMAL,nullable=true,nativeType=NUMBER,columnSize=4]}",
                Arrays.toString(schema.getTableByName("EMPLOYEES").getColumns()));

        assertEquals(
                "{JdbcColumn[name=DEPARTMENT_ID,columnNumber=0,type=DECIMAL,nullable=false,nativeType=NUMBER,columnSize=4],"
                        + "JdbcColumn[name=DEPARTMENT_NAME,columnNumber=1,type=VARCHAR,nullable=false,nativeType=VARCHAR2,columnSize=30],"
                        + "JdbcColumn[name=MANAGER_ID,columnNumber=2,type=DECIMAL,nullable=true,nativeType=NUMBER,columnSize=6],"
                        + "JdbcColumn[name=LOCATION_ID,columnNumber=3,type=DECIMAL,nullable=true,nativeType=NUMBER,columnSize=4]}",
                Arrays.toString(schema.getTableByName("DEPARTMENTS").getColumns()));
    }

    public void testExecuteQuery() throws Exception {
        if (!isConfigured()) {
            return;
        }
        Schema schema = getDataContext().getSchemaByName("HR");
        Table employeeTable = schema.getTableByName("EMPLOYEES");
        Table departmentsTable = schema.getTableByName("DEPARTMENTS");
        Relationship relationship = employeeTable.getRelationships(departmentsTable)[0];
        assertEquals(
                "Relationship[primaryTable=EMPLOYEES,primaryColumns={EMPLOYEE_ID},foreignTable=DEPARTMENTS,foreignColumns={MANAGER_ID}]",
                relationship.toString());

        Query q = new Query().from(new FromItem(JoinType.INNER, relationship)).select(
                employeeTable.getColumnByName("EMAIL"), departmentsTable.getColumnByName("DEPARTMENT_NAME"));
        q.getSelectClause().getItem(0).setAlias("e-mail");

        assertEquals(
                "SELECT \"EMPLOYEES\".\"EMAIL\" AS e-mail, \"DEPARTMENTS\".\"DEPARTMENT_NAME\" FROM HR.\"EMPLOYEES\" INNER JOIN HR.\"DEPARTMENTS\" ON \"EMPLOYEES\".\"EMPLOYEE_ID\" = \"DEPARTMENTS\".\"MANAGER_ID\"",
                q.toString());

        DataSet data = getDataContext().executeQuery(q);
        assertNotNull(data);
        TableModel tableModel = new DataSetTableModel(data);
        assertEquals(2, tableModel.getColumnCount());
        assertEquals(11, tableModel.getRowCount());
        assertEquals("JWHALEN", tableModel.getValueAt(0, 0).toString());
        assertEquals("Administration", tableModel.getValueAt(0, 1).toString());
    }

}