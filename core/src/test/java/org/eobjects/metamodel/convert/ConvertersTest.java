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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.eobjects.metamodel.MockUpdateableDataContext;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;

public class ConvertersTest extends TestCase {

    public void testAutoDetectConverters() throws Exception {
        final MockUpdateableDataContext decoratedDataContext = new MockUpdateableDataContext();
        final Table table = decoratedDataContext.getDefaultSchema().getTables()[0];
        Map<Column, TypeConverter<?, ?>> converters = Converters.autoDetectConverters(decoratedDataContext, table, 2);
        assertEquals(1, converters.size());
        assertEquals(
                "[Column[name=foo,columnNumber=0,type=VARCHAR,nullable=null,nativeType=null,columnSize=null]]",
                converters.keySet().toString());
        assertEquals(StringToIntegerConverter.class, converters.values().iterator().next().getClass());

        final UpdateableDataContext dc = Converters.addTypeConverters(decoratedDataContext, converters);

        DataSet ds = dc.query().from(table).select(table.getColumns()).execute();
        assertEquals(ConvertedDataSet.class, ds.getClass());
        assertTrue(ds.next());
        assertEquals("Row[values=[1, hello]]", ds.getRow().toString());
        assertEquals(Integer.class, ds.getRow().getValue(0).getClass());
        assertEquals(1, ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals(Integer.class, ds.getRow().getValue(0).getClass());
        assertEquals(2, ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals(Integer.class, ds.getRow().getValue(0).getClass());
        assertEquals(3, ds.getRow().getValue(0));
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value("foo", 4).value("bar", "mrrrrh").execute();
            }
        });

        // query the decorator
        ds = dc.query().from(table).select(table.getColumns()).where("foo").eq(4).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[4, mrrrrh]]", ds.getRow().toString());
        assertEquals(Integer.class, ds.getRow().getValue(0).getClass());
        assertEquals(4, ds.getRow().getValue(0));
        assertFalse(ds.next());
        ds.close();

        // query the decorated
        Object[] physicalRow = decoratedDataContext.getValues().get(3);
        assertEquals("[4, mrrrrh]", Arrays.toString(physicalRow));
        assertEquals(String.class, physicalRow[0].getClass());
    }

    public void testScenario() throws Exception {
        UpdateableDataContext dc = new MockUpdateableDataContext();
        List<Object[]> physicalValuesList = ((MockUpdateableDataContext) dc).getValues();
        assertEquals(3, physicalValuesList.size());
        for (Object[] physicalValues : physicalValuesList) {
            assertEquals("foo is expected to be string", String.class, physicalValues[0].getClass());
        }

        final Table table = dc.getDefaultSchema().getTables()[0];
        Map<Column, TypeConverter<?, ?>> converters = Converters.autoDetectConverters(dc, table, 1000);
        assertEquals(1, converters.size());
        dc = Converters.addTypeConverters(dc, converters);

        final Query q = dc.query().from(table).select("foo").toQuery();
        assertEquals("SELECT table.foo FROM schema.table", q.toSql());
        DataSet ds = dc.executeQuery(q);
        assertTrue(ds.next());
        assertEquals(1, ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals(2, ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals(3, ds.getRow().getValue(0));
        assertFalse(ds.next());

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value("foo", 4).value("bar", "heidiho!").execute();
            }
        });

        ds = dc.executeQuery(q);
        assertTrue(ds.next());
        assertEquals(1, ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals(2, ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals(3, ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals(4, ds.getRow().getValue(0));
        assertFalse(ds.next());
        
        assertEquals(4, physicalValuesList.size());
        for (Object[] physicalValues : physicalValuesList) {
            assertEquals("foo is expected to be string", String.class, physicalValues[0].getClass());
        }
        
        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value("foo", 5).value("bar", "hejsa...").execute();
                callback.update(table).where("foo").lessThan(3).value("foo", 100).execute();
            }
        });
        
        ds = dc.executeQuery(q);
        assertTrue(ds.next());
        assertEquals(3, ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals(4, ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals(5, ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals(100, ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals(100, ds.getRow().getValue(0));
        assertFalse(ds.next());
        
        assertEquals(5, physicalValuesList.size());
        for (Object[] physicalValues : physicalValuesList) {
            assertEquals("foo is expected to be string", String.class, physicalValues[0].getClass());
        }
    }
}
