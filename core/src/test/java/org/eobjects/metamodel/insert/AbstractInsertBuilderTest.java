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
package org.eobjects.metamodel.insert;

import java.util.Arrays;

import junit.framework.TestCase;

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.util.MutableRef;

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
