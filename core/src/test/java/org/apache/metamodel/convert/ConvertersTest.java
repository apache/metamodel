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
package org.apache.metamodel.convert;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.metamodel.MockUpdateableDataContext;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

public class ConvertersTest extends TestCase {

    public void testAutoDetectConverters() throws Exception {
        final MockUpdateableDataContext decoratedDataContext = new MockUpdateableDataContext();
        final Table table = decoratedDataContext.getDefaultSchema().getTables().get(0);
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

        final Table table = dc.getDefaultSchema().getTables().get(0);
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
