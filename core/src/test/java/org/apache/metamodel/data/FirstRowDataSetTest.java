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
package org.apache.metamodel.data;

import java.util.ArrayList;
import java.util.List;

import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.MutableColumn;

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
        
        ds.close();
    }

    public void testOffsetHigherThanSize() throws Exception {
        FirstRowDataSet ds = new FirstRowDataSet(dataSet, 8);
        assertFalse(ds.next());
        
        ds.close();
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
        
        ds.close();
    }
}
