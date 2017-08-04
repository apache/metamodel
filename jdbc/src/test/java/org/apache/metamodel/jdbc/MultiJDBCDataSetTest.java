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

import com.google.common.base.Stopwatch;
import org.apache.metamodel.CompositeDataContext;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.CreateTable;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.drop.DropTable;
import org.apache.metamodel.insert.InsertInto;
import org.apache.metamodel.schema.ColumnType;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.TimeUnit;

/**
 * A test case using two simple h2 in memory databases for executing single
 * query over both databases.
 */
public class MultiJDBCDataSetTest {

    public static final String DRIVER_CLASS = "org.h2.Driver";
    public static final String EMP_URL_MEMORY_DATABASE = "jdbc:h2:mem:emp";
    public static final String DEP_URL_MEMORY_DATABASE = "jdbc:h2:mem:dep";

    private Connection dep_conn;
    private UpdateableDataContext dep_dcon;

    private Connection emp_conn;
    private UpdateableDataContext emp_dcon;

    private int employeeSize = 10000;
    private int departmentSize = 1000;
    int employeesPerDepartment = employeeSize / departmentSize;

    private static final Logger logger = LoggerFactory.getLogger(MultiJDBCDataSetTest.class);

    @Before
    public void setup() throws Exception {
        Class.forName(DRIVER_CLASS);
        emp_conn = DriverManager.getConnection(EMP_URL_MEMORY_DATABASE);
        dep_conn = DriverManager.getConnection(DEP_URL_MEMORY_DATABASE);

        emp_dcon = new JdbcDataContext(emp_conn);
        dep_dcon = new JdbcDataContext(dep_conn);

        emp_dcon.executeUpdate(new CreateTable(emp_dcon.getDefaultSchema(), "employee").withColumn("id").ofType(
                ColumnType.INTEGER).asPrimaryKey().withColumn("name").ofType(ColumnType.VARCHAR).ofSize(200).withColumn(
                        "dep_id").ofType(ColumnType.INTEGER));

        for (int i = 0; i < employeeSize; i++) {
            emp_dcon.executeUpdate(new InsertInto(emp_dcon.getDefaultSchema().getTableByName("employee")).value("id", i)
                    .value("name", "emp" + i).value("dep_id", i % departmentSize));
        }

        dep_dcon.executeUpdate(new CreateTable(dep_dcon.getDefaultSchema(), "department").withColumn("id").ofType(
                ColumnType.INTEGER).asPrimaryKey().withColumn("name").ofType(ColumnType.VARCHAR).ofSize(200));

        for (int i = 0; i < departmentSize; i++) {
            dep_dcon.executeUpdate(new InsertInto(dep_dcon.getDefaultSchema().getTableByName("department")).value("id",
                    i).value("name", "dep" + i));
        }

    }

    @After
    public void tearDown() {
        dep_dcon.executeUpdate(new DropTable("department"));
        emp_dcon.executeUpdate(new DropTable("employee"));
    }

    @Test
    public void testJoin() {
        Stopwatch duration = Stopwatch.createStarted();
        CompositeDataContext compDcon = new CompositeDataContext(this.emp_dcon, this.dep_dcon);

        DataSet ds = compDcon.query().from("employee").innerJoin("department").on("dep_id", "id").selectAll().execute();
        int rowCount = 0;
        while (ds.next()) {
            Row row = ds.getRow();
            Assert.assertNotNull(row);
            rowCount++;
        }
        duration.stop();
        logger.info("Test duration was {} ms", duration.elapsed(TimeUnit.MILLISECONDS));

        Assert.assertEquals(employeeSize, rowCount);

    }

    @Test
    public void testSelectiveJoin() {
        Stopwatch duration = Stopwatch.createStarted();
        CompositeDataContext compDcon = new CompositeDataContext(this.emp_dcon, this.dep_dcon);

        DataSet ds = compDcon.query().from("employee").innerJoin("department").on("dep_id", "id").selectAll().where(
                compDcon.getTableByQualifiedLabel("department").getColumnByName("id")).eq(1).execute();
        int rowCount = 0;
        while (ds.next()) {
            Row row = ds.getRow();
            Assert.assertNotNull(row);
            rowCount++;
        }
        duration.stop();
        logger.info("Test duration was {} ms", duration.elapsed(TimeUnit.MILLISECONDS));

        Assert.assertEquals(employeesPerDepartment, rowCount);
    }

}
