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
package org.eobjects.metamodel.convert;

import java.util.List;

import junit.framework.TestCase;

import org.eobjects.metamodel.MockUpdateableDataContext;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.schema.Column;

public class ConvertedRowInsertionInterceptorTest extends TestCase {

	public void testConvertedInsert() throws Exception {
		MockUpdateableDataContext source = new MockUpdateableDataContext();
		Column fooColumn = source.getColumnByQualifiedLabel("schema.table.foo");
		assertNotNull(fooColumn);

		UpdateableDataContext intercepted = Converters.addTypeConverter(source,
				fooColumn, new StringToIntegerConverter());

		final List<Object[]> values = source.getValues();

		assertEquals(3, values.size());

		intercepted.executeUpdate(new UpdateScript() {
			@Override
			public void run(UpdateCallback callback) {
				callback.insertInto("schema.table").value(0, 1).value(1, "2")
						.execute();
				callback.insertInto("schema.table").value(0, 3).value(1, "4")
						.execute();
			}
		});

		assertEquals(5, values.size());
		assertEquals("1", values.get(3)[0]);
		assertEquals("2", values.get(3)[1]);
		assertEquals("3", values.get(4)[0]);
		assertEquals("4", values.get(4)[1]);
	}
}
