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

package org.eobjects.metamodel.data;

import org.eobjects.metamodel.MetaModelTestCase;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

public class RowTest extends MetaModelTestCase {

    public void testRow() throws Exception {
        Schema schema = getExampleSchema();
        Table projectTable = schema.getTableByName(TABLE_PROJECT);
        SelectItem item = new SelectItem(projectTable.getColumns()[0]);
        SelectItem[] items = { item };
        DataSetHeader header = new CachingDataSetHeader(items);
        Object[] values = { "foobar" };
        Row row = new DefaultRow(header, values);
        assertEquals("Row[values=[foobar]]", row.toString());
        assertEquals("foobar", row.getValue(0));
        assertEquals("foobar", row.getValues()[0]);
        assertEquals("foobar", row.getValue(item));
        assertEquals(item, row.getSelectItems()[0]);
    }

    public void testGetSubSelection() throws Exception {
        Schema schema = getExampleSchema();
        Table projectTable = schema.getTableByName(TABLE_PROJECT);
        SelectItem item1 = new SelectItem(projectTable.getColumns()[0]);
        SelectItem item2 = new SelectItem(projectTable.getColumns()[0]);
        SelectItem[] items = { item1, item2 };
        DataSetHeader header = new CachingDataSetHeader(items);
        Object[] values = { "foo", "bar" };
        Row row = new DefaultRow(header, values);
        row = row.getSubSelection(new SimpleDataSetHeader(new SelectItem[] { item1 }));
        assertEquals(1, row.getSelectItems().length);
        assertEquals(1, row.getValues().length);
        assertEquals("foo", row.getValue(0));
    }
}