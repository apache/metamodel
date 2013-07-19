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
package org.apache.metamodel.schema;

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