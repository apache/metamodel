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

package org.eobjects.metamodel.schema;

import junit.framework.TestCase;

public class MutableColumnTest extends TestCase {

	/**
	 * Tests that the following (general) rules apply to the object:
	 * 
	 * <li>the hashcode is the same when run twice on an unaltered object</li>
	 * <li>if o1.equals(o2) then this condition must be true: o1.hashCode() ==
	 * 02.hashCode()
	 */
	public void testEqualsAndHashCode() throws Exception {
		Column column1 = new MutableColumn("foo");
		Column column2 = new MutableColumn("foo");

		assertEquals(column1.hashCode(), column2.hashCode());
		assertEquals(column1, column2);

		column2 = new MutableColumn("bar");
		assertFalse(column1.equals(column2));

		column2 = new MutableColumn("foo", ColumnType.VARBINARY);
		assertFalse(column1.equals(column2));

		column1 = new MutableColumn("foo", ColumnType.VARBINARY);
		assertTrue(column1.equals(column2));
	}

	public void testQualifiedLabel() throws Exception {
		MutableSchema s = new MutableSchema("FOO_SCHEMA");
		MutableTable t = new MutableTable("FOO_TABLE");
		MutableColumn c = new MutableColumn("FOO_COLUMN");

		assertEquals("FOO_COLUMN", c.getQualifiedLabel());
		t.addColumn(c);
		c.setTable(t);
		assertEquals("FOO_TABLE.FOO_COLUMN", c.getQualifiedLabel());
		s.addTable(t);
		t.setSchema(s);
		assertEquals("FOO_SCHEMA.FOO_TABLE.FOO_COLUMN", c.getQualifiedLabel());

		s.setName("new_schema_name");
		assertEquals("new_schema_name.FOO_TABLE.FOO_COLUMN",
				c.getQualifiedLabel());
	}
}