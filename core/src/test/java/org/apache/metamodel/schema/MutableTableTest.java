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

import java.util.Arrays;

import junit.framework.TestCase;

public class MutableTableTest extends TestCase {

    /**
     * Tests that the following (general) rules apply to the object:
     * 
     * <li>the hashcode is the same when run twice on an unaltered object</li>
     * <li>if o1.equals(o2) then this condition must be true: o1.hashCode() ==
     * 02.hashCode()
     */
    public void testEqualsAndHashCode() throws Exception {
        MutableTable table1 = new MutableTable("Foo").addColumn(new MutableColumn("col1"));
        MutableTable table2 = new MutableTable("Foo").addColumn(new MutableColumn("col1"));

        assertFalse(table2.equals(null));
        assertEquals(table1.hashCode(), table2.hashCode());
        assertEquals(table1, table2);

        table2.addColumn(new MutableColumn("bar"));
        assertFalse(table1.equals(table2));

        int table1hash = table1.hashCode();
        int table2hash = table2.hashCode();
        assertTrue(table1hash + "==" + table2hash, table1hash == table2hash);
    }

    public void testGetColumnsOfType() throws Exception {
        MutableTable t = new MutableTable("foo");
        t.addColumn(new MutableColumn("b").setType(ColumnType.VARCHAR));
        t.addColumn(new MutableColumn("a").setType(ColumnType.VARCHAR));
        t.addColumn(new MutableColumn("r").setType(ColumnType.INTEGER));

        Column[] cols = t.getColumnsOfType(ColumnType.VARCHAR);
        assertEquals(2, cols.length);
        assertEquals("b", cols[0].getName());
        assertEquals("a", cols[1].getName());

        cols = t.getColumnsOfType(ColumnType.INTEGER);
        assertEquals(1, cols.length);
        assertEquals("r", cols[0].getName());

        cols = t.getColumnsOfType(ColumnType.FLOAT);
        assertEquals(0, cols.length);
    }

    public void testGetIndexedColumns() throws Exception {
        MutableTable t = new MutableTable("foo");
        t.addColumn(new MutableColumn("b").setIndexed(true));
        t.addColumn(new MutableColumn("a").setIndexed(false));
        t.addColumn(new MutableColumn("r").setIndexed(true));
        Column[] indexedColumns = t.getIndexedColumns();
        assertEquals(
                "[Column[name=b,columnNumber=0,type=null,nullable=null,nativeType=null,columnSize=null], Column[name=r,columnNumber=0,type=null,nullable=null,nativeType=null,columnSize=null]]",
                Arrays.toString(indexedColumns));
        for (Column column : indexedColumns) {
            assertTrue(column.isIndexed());
        }
    }

    public void testGetColumnByName() throws Exception {
        MutableTable t = new MutableTable("foobar");
        t.addColumn(new MutableColumn("Foo"));
        t.addColumn(new MutableColumn("FOO"));
        t.addColumn(new MutableColumn("bar"));

        assertEquals("Foo", t.getColumnByName("Foo").getName());
        assertEquals("FOO", t.getColumnByName("FOO").getName());
        assertEquals("bar", t.getColumnByName("bar").getName());

        // picking the first alternative that matches case insensitively
        assertEquals("Foo", t.getColumnByName("fOO").getName());
    }
}