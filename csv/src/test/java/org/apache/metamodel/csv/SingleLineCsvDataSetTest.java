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
package org.apache.metamodel.csv;

import java.io.File;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.Table;

public class SingleLineCsvDataSetTest extends TestCase {

    public void testGetValueInNonPhysicalOrder() throws Exception {
        CsvConfiguration configuration = new CsvConfiguration(1, true, false);
        CsvDataContext dc = new CsvDataContext(new File("src/test/resources/csv_people.csv"), configuration);

        DataSet dataSet = dc.query().from("csv_people.csv").select("age", "name").execute();

        assertTrue(dataSet.next());
        assertEquals("Row[values=[18, mike]]", dataSet.getRow().toString());

        assertEquals("mike", dataSet.getRow().getValue(dc.getColumnByQualifiedLabel("name")));

        assertTrue(dataSet.next());
        assertEquals("Row[values=[19, michael]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[18, peter]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[17, bob]]", dataSet.getRow().toString());

        dataSet.close();
    }

    public void testMalformedLineParsing() throws Exception {
        CsvConfiguration configuration = new CsvConfiguration(1, false, false);
        CsvDataContext dc = new CsvDataContext(new File("src/test/resources/csv_malformed_line.txt"), configuration);

        Table table = dc.getDefaultSchema().getTable(0);
        DataSet ds = dc.query().from(table).selectAll().execute();
        assertTrue(ds.next());
        assertEquals("[foo, bar, baz]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[\", null, null]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[hello, there, world]", Arrays.toString(ds.getRow().getValues()));
        assertFalse(ds.next());
        ds.close();
    }
}
