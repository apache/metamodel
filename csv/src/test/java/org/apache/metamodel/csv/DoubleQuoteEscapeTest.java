/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.csv;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.metamodel.data.DataSet;
import org.junit.Test;

public class DoubleQuoteEscapeTest {

    @Test
    public void testDoubleQuoteEscape() throws Exception {
        CsvConfiguration configuration = new CsvConfiguration(1, "UTF-8", ',', '"', '"');
        CsvDataContext dc = new CsvDataContext(new File("src/test/resources/csv_doublequoteescape.csv"), configuration);

        DataSet dataSet = dc.query().from("csv_doublequoteescape.csv").select("age", "name").execute();

        assertTrue(dataSet.next());
        assertEquals("Row[values=[18, mi\"ke]]", dataSet.getRow().toString());

        assertEquals("mi\"ke", dataSet.getRow().getValue(dc.getColumnByQualifiedLabel("name")));

        assertTrue(dataSet.next());
        assertEquals("Row[values=[19, mic\"hael]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[18, pet\"er]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[18, barbar\"a, \"\"barb]]", dataSet.getRow().toString());

        dataSet.close();
    }

    @Test
    public void testWeirdQuotes() throws Exception {
        CsvConfiguration configuration = new CsvConfiguration(1, "UTF-8", ',', '\\', '\\');
        CsvDataContext dc = new CsvDataContext(new File("src/test/resources/csv_weirdquotes.csv"), configuration);

        DataSet dataSet = dc.query().from("csv_weirdquotes.csv").select("age", "name").execute();

        assertTrue(dataSet.next());
        assertEquals("Row[values=[18, mi\\ke]]", dataSet.getRow().toString());

        assertEquals("mi\\ke", dataSet.getRow().getValue(dc.getColumnByQualifiedLabel("name")));

        assertTrue(dataSet.next());
        assertEquals("Row[values=[19, mic\\hael]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[18, pet\\er]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[18, barbar\\a, \\\\barb]]", dataSet.getRow().toString());

        dataSet.close();
    }

}
