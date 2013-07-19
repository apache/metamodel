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
package org.apache.metamodel.insert;

import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.util.MutableRef;

public class AbstractInsertBuilderTest extends TestCase {

	public void testInsertValues() throws Exception {
		final MutableRef<Boolean> executed = new MutableRef<Boolean>(false);
		final MutableTable table = new MutableTable("foo");
		table.addColumn(new MutableColumn("foo"));
		table.addColumn(new MutableColumn("bar"));
		table.addColumn(new MutableColumn("baz"));
		RowInsertionBuilder insertBuilder = new AbstractRowInsertionBuilder<UpdateCallback>(
				null, table) {
			@Override
			public void execute() throws MetaModelException {
				assertEquals("[1, 2, 3]", Arrays.toString(getValues()));
				executed.set(true);
			}
		};

		assertFalse(executed.get().booleanValue());

		insertBuilder.value(0, 1).value("bar", 2)
				.value(table.getColumnByName("baz"), 3).execute();

		assertTrue(executed.get());
		
		assertEquals("Row[values=[1, 2, 3]]", insertBuilder.toRow().toString());
		
	}

	public void testIllegalArguments() throws Exception {
		final MutableTable table = new MutableTable("foo");
		table.addColumn(new MutableColumn("foo"));
		RowInsertionBuilder insertBuilder = new AbstractRowInsertionBuilder<UpdateCallback>(
				null, table) {
			@Override
			public void execute() throws MetaModelException {
			}
		};
		
		try {
			insertBuilder.value((Column)null, "foo");
			fail("Exception expected");
		} catch (IllegalArgumentException e) {
			assertEquals("Column cannot be null", e.getMessage());
		}

		try {
			insertBuilder.value("hmm", "foo");
			fail("Exception expected");
		} catch (IllegalArgumentException e) {
			assertEquals("No such column in table: hmm, available columns are: [Column[name=foo,columnNumber=0,type=null,nullable=null,nativeType=null,columnSize=null]]", e.getMessage());
		}

		try {
			insertBuilder.value(4, "foo");
			fail("Exception expected");
		} catch (ArrayIndexOutOfBoundsException e) {
            assertTrue("4".equals(e.getMessage())
                    || "Array index out of range: 4".equals(e.getMessage()));
		}
	}
}
