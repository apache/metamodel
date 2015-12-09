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

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MockUpdateableDataContext;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.InMemoryDataSet;
import org.apache.metamodel.data.WrappingDataSet;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

public class ConvertedDataSetInterceptorTest extends TestCase {

    public void testConvertedQuery() throws Exception {
        UpdateableDataContext dc = new MockUpdateableDataContext();
        Column fooColumn = dc.getColumnByQualifiedLabel("schema.table.foo");
        assertNotNull(fooColumn);

        dc = Converters.addTypeConverter(dc, fooColumn, new StringToIntegerConverter());

        Table table = dc.getDefaultSchema().getTableByName("table");
        Query query = dc.query().from(table).select(table.getColumns()).toQuery();
        assertEquals("SELECT table.foo, table.bar FROM schema.table", query.toSql());

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

    @SuppressWarnings("resource")
    public void testNonConvertedQuery() throws Exception {
        MockUpdateableDataContext source = new MockUpdateableDataContext();
        Column fooColumn = source.getColumnByQualifiedLabel("schema.table.foo");
        assertNotNull(fooColumn);

        Map<Column, TypeConverter<?, ?>> converters = new HashMap<Column, TypeConverter<?, ?>>();
        converters.put(fooColumn, new StringToIntegerConverter());
        DataContext converted = Converters.addTypeConverter(source, fooColumn, new StringToIntegerConverter());

        // only select "bar" which is not converted
        Table table = converted.getDefaultSchema().getTableByName("table");
        Query query = converted.query().from(table).select("bar").toQuery();
        assertEquals("SELECT table.bar FROM schema.table", query.toSql());

        DataSet ds = converted.executeQuery(query);
        while (ds instanceof WrappingDataSet) {
            ds = ((WrappingDataSet) ds).getWrappedDataSet();
        }
        ds.close();
        assertEquals(InMemoryDataSet.class, ds.getClass());
    }
}
