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

import java.util.ArrayList;
import java.util.List;

import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.MutableColumn;

import junit.framework.TestCase;

public class FirstRowDataSetTest extends TestCase {

    private List<Row> rows;
    private SelectItem[] items = new SelectItem[] { new SelectItem(new MutableColumn("foobar")) };
    private DataSetHeader header = new SimpleDataSetHeader(items);
    private InMemoryDataSet dataSet;

    protected void setUp() throws Exception {
        rows = new ArrayList<Row>();
        rows.add(new DefaultRow(header, new Object[] { 1 }));
        rows.add(new DefaultRow(header, new Object[] { 2 }));
        rows.add(new DefaultRow(header, new Object[] { 3 }));
        rows.add(new DefaultRow(header, new Object[] { 4 }));
        rows.add(new DefaultRow(header, new Object[] { 5 }));
        dataSet = new InMemoryDataSet(header, rows);
    };

    public void testHighestPossibleOffset() throws Exception {
        FirstRowDataSet ds = new FirstRowDataSet(dataSet, 5);
        assertTrue(ds.next());
        assertEquals(5, ds.getRow().getValue(0));
        assertFalse(ds.next());
    }

    public void testOffsetHigherThanSize() throws Exception {
        FirstRowDataSet ds = new FirstRowDataSet(dataSet, 8);
        assertFalse(ds.next());
    }

    public void testOneOffset() throws Exception {
        FirstRowDataSet ds = new FirstRowDataSet(dataSet, 1);
        assertTrue(ds.next());
        assertEquals(1, ds.getRow().getValue(0));
        ds.close();
    }

    public void testVanillaScenario() throws Exception {
        FirstRowDataSet ds = new FirstRowDataSet(dataSet, 2);
        assertTrue(ds.next());
        assertEquals(2, ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals(3, ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals(4, ds.getRow().getValue(0));
        assertTrue(ds.next());
        assertEquals(5, ds.getRow().getValue(0));
        assertFalse(ds.next());
    }
}
