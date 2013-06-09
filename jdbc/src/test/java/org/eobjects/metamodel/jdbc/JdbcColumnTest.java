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

import java.sql.Connection;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;

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
