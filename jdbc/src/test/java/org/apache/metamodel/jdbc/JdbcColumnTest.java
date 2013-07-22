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

import java.sql.Connection;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;

public class JdbcColumnTest extends JdbcTestCase {

	public void testEqualsDisconected() throws Exception {
		Connection con1 = getTestDbConnection();
		DataContext dc = new JdbcDataContext(con1);
		Column col1 = dc.getDefaultSchema().getTableByName("EMPLOYEES").getColumnByName("EMPLOYEENUMBER");
		con1.close();

		Connection con2 = getTestDbConnection();
		assertTrue(con1 != con2);
		dc = new JdbcDataContext(con2);
		Column col2 = dc.getDefaultSchema().getTableByName("EMPLOYEES").getColumnByName("EMPLOYEENUMBER");

		assertEquals(col1, col2);
		assertTrue(new SelectItem(col1).equals(new SelectItem(col2)));
		assertTrue(new SelectItem(col1).setAlias("foo").equalsIgnoreAlias(new SelectItem(col2).setAlias("bar")));

		con2.close();

		assertEquals(col1, col2);
		assertTrue(new SelectItem(col1).equals(new SelectItem(col2)));
		assertTrue(new SelectItem(col1).setAlias("foo").equalsIgnoreAlias(new SelectItem(col2).setAlias("bar")));

		try {
			col2.isIndexed();
			fail("Exception expected");
		} catch (MetaModelException e) {
			assertEquals("Could not load indexes: Connection is closed", e.getMessage());
		}
	}
}
