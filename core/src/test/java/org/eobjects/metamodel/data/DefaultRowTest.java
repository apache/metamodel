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
package org.eobjects.metamodel.data;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;

import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.util.FileHelper;

import junit.framework.TestCase;

public class DefaultRowTest extends TestCase {

    SelectItem[] items = new SelectItem[] { new SelectItem(new MutableColumn("foo")),
            new SelectItem(new MutableColumn("bar")) };
    Object[] values = new Object[] { "foo", "bar" };

    public void testDeserializeBackwardsCompatible() throws Exception {
        Object obj;
        FileInputStream fileIn = new FileInputStream("src/test/resources/metamodel-3.0-default-row.ser");
        try {
            ObjectInputStream ois = new ObjectInputStream(fileIn);
            obj = ois.readObject();
            ois.close();
        } finally {
            FileHelper.safeClose(fileIn);
        }

        assertTrue(obj instanceof Row);
        assertTrue(obj instanceof DefaultRow);

        Row row = (Row) obj;
        assertEquals(2, row.size());
        SelectItem[] selectItems = row.getSelectItems();
        assertEquals("foo", selectItems[0].toString());
        assertEquals("bar", selectItems[1].toString());

        assertEquals("foo", row.getValue(0));
        assertEquals("bar", row.getValue(1));

        assertEquals(Style.NO_STYLE, row.getStyle(0));
        assertEquals(Style.NO_STYLE, row.getStyle(1));
    }

    public void testGetValueOfColumn() throws Exception {
        DefaultRow row = new DefaultRow(new SimpleDataSetHeader(items), values);
        assertEquals("foo", row.getValue(new MutableColumn("foo")));
        assertNull(row.getValue(new MutableColumn("hello world")));
    }

    public void testCustomStyles() throws Exception {
        Style[] styles = new Style[] { Style.NO_STYLE, new StyleBuilder().bold().create() };
        DefaultRow row = new DefaultRow(new SimpleDataSetHeader(items), values, styles);

        Field field = DefaultRow.class.getDeclaredField("_styles");
        assertNotNull(field);

        field.setAccessible(true);
        assertNotNull(field.get(row));

        assertEquals(Style.NO_STYLE, row.getStyle(0));
        assertEquals("font-weight: bold;", row.getStyle(1).toCSS());
    }

    public void testNoStylesReference() throws Exception {
        Style[] styles = new Style[] { Style.NO_STYLE, Style.NO_STYLE };
        DefaultRow row = new DefaultRow(new SimpleDataSetHeader(items), values, styles);

        Field field = DefaultRow.class.getDeclaredField("_styles");
        assertNotNull(field);

        field.setAccessible(true);
        assertNull(field.get(row));

        assertEquals(Style.NO_STYLE, row.getStyle(0));
        assertEquals(Style.NO_STYLE, row.getStyle(items[0]));
        assertEquals(Style.NO_STYLE, row.getStyle(items[0].getColumn()));
    }

    public void testNullStyle() throws Exception {
        Style[] styles = new Style[] { Style.NO_STYLE, null };

        try {
            new DefaultRow(new SimpleDataSetHeader(items), values, styles);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertEquals("Elements in the style array cannot be null", e.getMessage());
        }
    }
}
