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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.MutableColumn;
import org.junit.Test;

import com.google.common.collect.Lists;

public class UnionDataSetTest {

    @Test
    public void testVanillaUnion() {
        // data set 1
        final DataSetHeader header1 =
                new SimpleDataSetHeader(
                        Lists.newArrayList(new MutableColumn("foo1"), new MutableColumn("bar1")).stream()
                                .map(SelectItem::new).collect(Collectors.toList()));
        final Row row1 = new DefaultRow(header1, new Object[] { "1", "2" });
        final Row row2 = new DefaultRow(header1, new Object[] { "3", "4" });
        final DataSet ds1 = new InMemoryDataSet(header1, row1, row2);

        // data set 2
        final DataSetHeader header2 =
                new SimpleDataSetHeader(
                        Lists.newArrayList( new MutableColumn("foo2"), new MutableColumn("bar2") ).stream()
                                .map(SelectItem::new).collect(Collectors.toList()));
        final Row row3 = new DefaultRow(header2, new Object[] { "5", "6" });
        final DataSet ds2 = new InMemoryDataSet(header2, row3);

        // data set 3
        final DataSetHeader header3 =
                new SimpleDataSetHeader(
                        Lists.newArrayList( new MutableColumn("foo3"), new MutableColumn("bar3") ).stream()
                                .map(SelectItem::new).collect(Collectors.toList()));
        final Row row4 = new DefaultRow(header2, new Object[] { "7", "8" });
        final DataSet ds3 = new InMemoryDataSet(header3, row4);

        final DataSetHeader unionHeader =
                new SimpleDataSetHeader(
                        Lists.newArrayList( new MutableColumn("fooUnion"), new MutableColumn("barUnion") ).stream()
                            .map(SelectItem::new).collect(Collectors.toList()));
        final DataSet unionDataSet = UnionDataSet.ofDataSets(unionHeader, Arrays.asList(ds1, ds2, ds3));
        assertTrue(unionDataSet.next());
        assertEquals("Row[values=[1, 2]]", unionDataSet.getRow().toString());
        assertTrue(unionDataSet.next());
        assertEquals("Row[values=[3, 4]]", unionDataSet.getRow().toString());
        assertTrue(unionDataSet.next());
        assertEquals("Row[values=[5, 6]]", unionDataSet.getRow().toString());
        assertTrue(unionDataSet.next());
        assertEquals("Row[values=[7, 8]]", unionDataSet.getRow().toString());
        assertFalse(unionDataSet.next());
        unionDataSet.close();
    }
}
