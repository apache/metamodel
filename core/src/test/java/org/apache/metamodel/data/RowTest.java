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

import com.google.common.collect.Lists;
import org.apache.metamodel.MetaModelTestCase;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class RowTest extends MetaModelTestCase {

    public void testRow() throws Exception {
        Schema schema = getExampleSchema();
        Table projectTable = schema.getTableByName(TABLE_PROJECT);
        SelectItem item = new SelectItem(projectTable.getColumn(0));
        DataSetHeader header = new CachingDataSetHeader(Lists.newArrayList(item));
        Object[] values = { "foobar" };
        Row row = new DefaultRow(header, values);
        assertEquals("Row[values=[foobar]]", row.toString());
        assertEquals("foobar", row.getValue(0));
        assertEquals("foobar", row.getValues()[0]);
        assertEquals("foobar", row.getValue(item));
        assertEquals(item, row.getSelectItems().get(0));
    }

    public void testGetSubSelection() throws Exception {
        Schema schema = getExampleSchema();
        Table projectTable = schema.getTableByName(TABLE_PROJECT);
        SelectItem item1 = new SelectItem(projectTable.getColumn(0));
        SelectItem item2 = new SelectItem(projectTable.getColumn(0));
        DataSetHeader header = new CachingDataSetHeader(Lists.newArrayList(item1,item2));
        Object[] values = { "foo", "bar" };
        Row row = new DefaultRow(header, values);
        row = row.getSubSelection(new SimpleDataSetHeader(new SelectItem[] { item1 }));
        assertEquals(1, row.getSelectItems().size());
        assertEquals(1, row.getValues().length);
        assertEquals("foo", row.getValue(0));
    }
}