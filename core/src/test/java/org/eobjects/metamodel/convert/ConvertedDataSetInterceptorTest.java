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

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.MockUpdateableDataContext;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.InMemoryDataSet;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;

public class ConvertedDataSetInterceptorTest extends TestCase {

	public void testConvertedQuery() throws Exception {
		UpdateableDataContext dc = new MockUpdateableDataContext();
		Column fooColumn = dc.getColumnByQualifiedLabel("schema.table.foo");
		assertNotNull(fooColumn);

		dc = Converters.addTypeConverter(dc, fooColumn,
				new StringToIntegerConverter());

		Table table = dc.getDefaultSchema().getTableByName("table");
		Query query = dc.query().from(table).select(table.getColumns())
				.toQuery();
		assertEquals("SELECT table.foo, table.bar FROM schema.table",
				query.toSql());

		DataSet ds = dc.executeQuery(query);
		assertEquals(ConvertedDataSet.class, ds.getClass());

		assertTrue(ds.next());
		assertEquals("Row[values=[1, hello]]", ds.getRow().toString());
		assertEquals(Integer.class, ds.getRow().getValue(0).getClass());
		assertEquals(String.class, ds.getRow().getValue(1).getClass());

		assertTrue(ds.next());
		assertEquals("Row[values=[2, there]]", ds.getRow().toString());
		assertEquals(Integer.class, ds.getRow().getValue(0).getClass());
		assertEquals(String.class, ds.getRow().getValue(1).getClass());

		assertTrue(ds.next());
		assertEquals("Row[values=[3, world]]", ds.getRow().toString());
		assertEquals(Integer.class, ds.getRow().getValue(0).getClass());
		assertEquals(String.class, ds.getRow().getValue(1).getClass());

		assertFalse(ds.next());
		ds.close();
	}

	public void testNonConvertedQuery() throws Exception {
		MockUpdateableDataContext source = new MockUpdateableDataContext();
		Column fooColumn = source.getColumnByQualifiedLabel("schema.table.foo");
		assertNotNull(fooColumn);

		Map<Column, TypeConverter<?, ?>> converters = new HashMap<Column, TypeConverter<?, ?>>();
		converters.put(fooColumn, new StringToIntegerConverter());
		DataContext converted = Converters.addTypeConverter(source, fooColumn,
				new StringToIntegerConverter());

		// only select "bar" which is not converted
		Table table = converted.getDefaultSchema().getTableByName("table");
		Query query = converted.query().from(table).select("bar").toQuery();
		assertEquals("SELECT table.bar FROM schema.table", query.toSql());

		DataSet ds = converted.executeQuery(query);
		assertEquals(InMemoryDataSet.class, ds.getClass());

	}
}
