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
package org.apache.metamodel;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import junit.framework.TestCase;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class AbstractDataContextTest extends TestCase {

    private class MyDataContext extends AbstractDataContext {

        private final Map<String, Schema> _schemas;
        private final String _defaultSchemaName;

        public MyDataContext() {
            this("foobar", createSchema("barfoo"), createSchema("foobar"), createSchema("foo.bar"));
        }

        public MyDataContext(String defaultSchemaName, Schema... schemas) {
            _defaultSchemaName = defaultSchemaName;
            _schemas = new LinkedHashMap<String, Schema>();
            for (Schema schema : schemas) {
                _schemas.put(schema.getName(), schema);
            }
        }

        @Override
        public DataSet executeQuery(Query query) throws MetaModelException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String[] getSchemaNamesInternal() {
            return _schemas.keySet().toArray(new String[0]);
        }

        @Override
        protected String getDefaultSchemaName() {
            return _defaultSchemaName;
        }

        @Override
        protected Schema getSchemaByNameInternal(String name) {
            Schema schema = _schemas.get(name);
            if (schema == null) {
                throw new IllegalStateException("No such schema: " + name);
            }
            return schema;
        }
    }

    public void testGetTableWithQuotesInLabel() throws Exception {
        MutableSchema schema1 = new MutableSchema("foo");
        schema1.addTable(new MutableTable("bar.baz", schema1));
        MutableSchema schema2 = new MutableSchema("foo.bar");
        schema2.addTable(new MutableTable("baz", schema2));

        MyDataContext dc = new MyDataContext("foo", schema1, schema2);
        assertEquals("baz", dc.getTableByQualifiedLabel("foo.bar.baz").getName());

        assertEquals("baz", dc.getTableByQualifiedLabel("\"foo.bar\".baz").getName());
        assertEquals("baz", dc.getTableByQualifiedLabel("\"foo.bar\".\"baz\"").getName());
        assertEquals("bar.baz", dc.getTableByQualifiedLabel("foo.\"bar.baz\"").getName());
        assertEquals("bar.baz", dc.getTableByQualifiedLabel("\"foo\".\"bar.baz\"").getName());
    }

    public void testGetColumnWithQuotesInLabel() throws Exception {
        MutableSchema schema = new MutableSchema("foo");

        MutableTable table1 = new MutableTable("bar.baz", schema);
        schema.addTable(table1);
        table1.addColumn(new MutableColumn("buuh", table1));

        MutableTable table2 = new MutableTable("bar", schema);
        schema.addTable(table2);
        table2.addColumn(new MutableColumn("baz.buuh", table2));

        MyDataContext dc = new MyDataContext("foo", schema);
        assertEquals("buuh", dc.getColumnByQualifiedLabel("foo.bar.baz.buuh").getName());

        assertEquals("buuh", dc.getColumnByQualifiedLabel("foo.\"bar.baz\".buuh").getName());
        assertEquals("buuh", dc.getColumnByQualifiedLabel("foo.\"bar.baz\".\"buuh\"").getName());
        assertEquals("baz.buuh", dc.getColumnByQualifiedLabel("foo.bar.\"baz.buuh\"").getName());
        assertEquals("baz.buuh", dc.getColumnByQualifiedLabel("\"foo\".bar.\"baz.buuh\"").getName());
    }

    public void testGetColumnByQualifiedLabel() throws Exception {
        MyDataContext dc = new MyDataContext();
        Column result;

        result = dc.getColumnByQualifiedLabel("foobar.tab.le.col1");
        result = dc.getColumnByQualifiedLabel("blabla.tab.le.col4");
        result = dc.getColumnByQualifiedLabel("FOOBAR.TABLE.COL3");
        assertNull(result);

        result = dc.getColumnByQualifiedLabel("foobar.table.col1");
        assertEquals("col1", result.getName());
        assertEquals("table", result.getTable().getName());
        assertEquals("foobar", result.getTable().getSchema().getName());

        result = dc.getColumnByQualifiedLabel("foo.bar.table.col1");
        assertEquals("col1", result.getName());
        assertEquals("table", result.getTable().getName());
        assertEquals("foo.bar", result.getTable().getSchema().getName());

        result = dc.getColumnByQualifiedLabel("foobar.tab.le.col3");
        assertEquals("col3", result.getName());
        assertEquals("tab.le", result.getTable().getName());
        assertEquals("foobar", result.getTable().getSchema().getName());

        result = dc.getColumnByQualifiedLabel("FOO.BAR.tab.le.col3");
        assertEquals("col3", result.getName());
        assertEquals("tab.le", result.getTable().getName());
        assertEquals("foo.bar", result.getTable().getSchema().getName());

        result = dc.getColumnByQualifiedLabel("tab.le.col3");
        assertEquals("col3", result.getName());
        assertEquals("tab.le", result.getTable().getName());
        assertEquals("foobar", result.getTable().getSchema().getName());
    }

    public void testGetTableByQualfiedLabelSchemaNameInTableName() throws Exception {
        AbstractDataContext dc = new AbstractDataContext() {
            @Override
            public DataSet executeQuery(Query query) throws MetaModelException {
                return null;
            }

            @Override
            protected String[] getSchemaNamesInternal() {
                return new String[] { "test" };
            }

            @Override
            protected Schema getSchemaByNameInternal(String name) {
                MutableSchema sch = new MutableSchema("test");
                sch.addTable(new MutableTable("test_table1").setSchema(sch));
                sch.addTable(new MutableTable("test_table2").setSchema(sch));
                sch.addTable(new MutableTable("test_table3").setSchema(sch));
                return sch;
            }

            @Override
            protected String getDefaultSchemaName() {
                return "test";
            }
        };

        assertEquals("test_table1", dc.getTableByQualifiedLabel("test_table1").getName());
        assertEquals("test_table2", dc.getTableByQualifiedLabel("test_table2").getName());
        assertEquals("test_table3", dc.getTableByQualifiedLabel("test_table3").getName());
    }

    public void testGetTableByQualifiedLabel() throws Exception {
        MyDataContext dc = new MyDataContext();

        Table result;

        result = dc.getTableByQualifiedLabel("FOOBAR.table");
        assertEquals("table", result.getName());
        assertEquals("foobar", result.getSchema().getName());

        result = dc.getTableByQualifiedLabel("table");
        assertEquals("table", result.getName());
        assertEquals("foobar", result.getSchema().getName());

        result = dc.getTableByQualifiedLabel("foo.bar.table");
        assertEquals("table", result.getName());
        assertEquals("foo.bar", result.getSchema().getName());

        result = dc.getTableByQualifiedLabel("foobar.tab.le");
        assertEquals("tab.le", result.getName());
        assertEquals("foobar", result.getSchema().getName());

        result = dc.getTableByQualifiedLabel("foo.bar.tab.le");
        assertEquals("tab.le", result.getName());
        assertEquals("foo.bar", result.getSchema().getName());

        result = dc.getTableByQualifiedLabel("foo.table");
        assertNull(result);
    }

    public void testGetSchemas() throws Exception {
        MyDataContext dc = new MyDataContext();
        Schema[] schemas = dc.getSchemas();
        assertEquals("[Schema[name=barfoo], Schema[name=foo.bar], Schema[name=foobar]]", Arrays.toString(schemas));

        dc.refreshSchemas();
        schemas = dc.getSchemas();
        assertEquals("[Schema[name=barfoo], Schema[name=foo.bar], Schema[name=foobar]]", Arrays.toString(schemas));
    }

    public void testGetColumnByQualifiedLabelWithNameOverlaps() throws Exception {
        AbstractDataContext dc = new AbstractDataContext() {

            @Override
            public DataSet executeQuery(Query query) throws MetaModelException {
                throw new UnsupportedOperationException();
            }

            @Override
            protected String[] getSchemaNamesInternal() {
                return new String[] { "sch" };
            }

            @Override
            protected Schema getSchemaByNameInternal(String name) {
                MutableSchema schema = new MutableSchema("sch");
                MutableTable table1 = new MutableTable("tab");
                MutableTable table2 = new MutableTable("tab_le");
                MutableTable table3 = new MutableTable("table");
                MutableTable table4 = new MutableTable("tabl_e");
                schema.addTable(table1.addColumn(new MutableColumn("col").setTable(table1)));
                schema.addTable(table2.addColumn(new MutableColumn("col").setTable(table2)));
                schema.addTable(table3.addColumn(new MutableColumn("col").setTable(table3)));
                schema.addTable(table4.addColumn(new MutableColumn("col").setTable(table4)));
                return schema;
            }

            @Override
            protected String getDefaultSchemaName() {
                return "sch";
            }
        };

        assertEquals("tab.col", dc.getColumnByQualifiedLabel("sch.tab.col").getQualifiedLabel());
        assertEquals("table.col", dc.getColumnByQualifiedLabel("sch.table.col").getQualifiedLabel());
        assertEquals("tab_le.col", dc.getColumnByQualifiedLabel("sch.tab_le.col").getQualifiedLabel());
        assertEquals("tabl_e.col", dc.getColumnByQualifiedLabel("sch.tabl_e.col").getQualifiedLabel());
    }

    public void testGetColumnByQualifiedLabelCaseInsensitive() throws Exception {
        MyDataContext dc = new MyDataContext();
        Column result = dc.getColumnByQualifiedLabel("FOOBAR.TABLE.COL1");
        assertNotNull(result);
        assertEquals("col1", result.getName());
    }

    private Schema createSchema(String name) {
        MutableSchema schema = new MutableSchema(name);
        MutableTable t1 = new MutableTable("table");
        MutableColumn col1 = new MutableColumn("col1");
        MutableColumn col2 = new MutableColumn("col2");
        t1.addColumn(col1).addColumn(col2);
        col1.setTable(t1);
        col2.setTable(t1);
        MutableTable t2 = new MutableTable("tab.le");
        MutableColumn col3 = new MutableColumn("col3");
        MutableColumn col4 = new MutableColumn("col4");
        t2.addColumn(col3).addColumn(col4);
        col3.setTable(t2);
        col4.setTable(t2);
        schema.addTable(t1).addTable(t2);
        t1.setSchema(schema);
        t2.setSchema(schema);
        return schema;
    }
}