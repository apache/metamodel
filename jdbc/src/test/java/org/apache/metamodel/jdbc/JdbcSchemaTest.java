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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.util.Arrays;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class JdbcSchemaTest extends JdbcTestCase {

	/**
	 * Ticket #248: Tables and Schemas need to be Serialiazable
	 */
	public void testSerialize() throws Exception {
		Connection connection = getTestDbConnection();
		DataContext dataContext = new JdbcDataContext(connection);
		Schema schema = dataContext.getDefaultSchema();
		assertTrue(schema instanceof JdbcSchema);

		File file = new File("src/test/resources/jdbcschema_serialized.dat");

		ObjectOutputStream objectOutputStream = new ObjectOutputStream(
				new FileOutputStream(file));

		// write schema before it has been lazy loaded (and ensure that it will
		// load before serialization)
		objectOutputStream.writeObject(schema);
		objectOutputStream.flush();
		objectOutputStream.close();

		assertEquals(
				"[CUSTOMERS, CUSTOMER_W_TER, DEPARTMENT_MANAGERS, DIM_TIME, EMPLOYEES, OFFICES, ORDERDETAILS, ORDERFACT, ORDERS, PAYMENTS, PRODUCTS, QUADRANT_ACTUALS, TRIAL_BALANCE]",
				Arrays.toString(schema.getTableNames().toArray()));

		connection.close();
		dataContext = null;
		schema = null;
		System.gc();
		System.runFinalization();

		ObjectInputStream objectInputStream = new ObjectInputStream(
				new FileInputStream(file));
		schema = (Schema) objectInputStream.readObject();
		objectInputStream.close();
		
		assertEquals("Schema[name=PUBLIC]", schema.toString());
		assertTrue(schema instanceof JdbcSchema);

		assertEquals(
				"[CUSTOMERS, CUSTOMER_W_TER, DEPARTMENT_MANAGERS, DIM_TIME, EMPLOYEES, OFFICES, ORDERDETAILS, ORDERFACT, ORDERS, PAYMENTS, PRODUCTS, QUADRANT_ACTUALS, TRIAL_BALANCE]",
				Arrays.toString(schema.getTableNames().toArray()));

		Table table = schema.getTableByName("CUSTOMERS");
		assertTrue(table instanceof JdbcTable);
		assertNotNull(table);
		assertEquals(
				"[CUSTOMERNUMBER, CUSTOMERNAME, CONTACTLASTNAME, CONTACTFIRSTNAME, PHONE, ADDRESSLINE1, ADDRESSLINE2, CITY, STATE, POSTALCODE, COUNTRY, SALESREPEMPLOYEENUMBER, CREDITLIMIT]",
				Arrays.toString(table.getColumnNames().toArray()));
	}

	public void testToSerializableForm() throws Exception {
		Connection connection = getTestDbConnection();
		DataContext dataContext = new JdbcDataContext(connection);
		Schema schema = dataContext.getDefaultSchema();
		schema = ((JdbcSchema) schema).toSerializableForm();
		connection.close();

		Table table = schema.getTableByName("CUSTOMERS");
		assertEquals(
				"[CUSTOMERNUMBER, CUSTOMERNAME, CONTACTLASTNAME, CONTACTFIRSTNAME, PHONE, ADDRESSLINE1, ADDRESSLINE2, CITY, STATE, POSTALCODE, COUNTRY, SALESREPEMPLOYEENUMBER, CREDITLIMIT]",
				Arrays.toString(table.getColumnNames().toArray()));

		assertEquals(
				"[Relationship[primaryTable=PRODUCTS,primaryColumns=[PRODUCTCODE],foreignTable=ORDERFACT,foreignColumns=[PRODUCTCODE]]]",
				Arrays.toString(schema.getRelationships().toArray()));
	}
}