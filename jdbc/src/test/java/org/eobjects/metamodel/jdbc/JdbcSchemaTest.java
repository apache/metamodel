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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.util.Arrays;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

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
				Arrays.toString(schema.getTableNames()));

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
				Arrays.toString(schema.getTableNames()));

		Table table = schema.getTableByName("CUSTOMERS");
		assertTrue(table instanceof JdbcTable);
		assertNotNull(table);
		assertEquals(
				"[CUSTOMERNUMBER, CUSTOMERNAME, CONTACTLASTNAME, CONTACTFIRSTNAME, PHONE, ADDRESSLINE1, ADDRESSLINE2, CITY, STATE, POSTALCODE, COUNTRY, SALESREPEMPLOYEENUMBER, CREDITLIMIT]",
				Arrays.toString(table.getColumnNames()));
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
				Arrays.toString(table.getColumnNames()));

		assertEquals(
				"[Relationship[primaryTable=PRODUCTS,primaryColumns=[PRODUCTCODE],foreignTable=ORDERFACT,foreignColumns=[PRODUCTCODE]]]",
				Arrays.toString(schema.getRelationships()));
	}
}